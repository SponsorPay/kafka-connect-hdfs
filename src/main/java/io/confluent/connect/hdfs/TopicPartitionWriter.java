/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.hdfs;

import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.errors.HiveMetaStoreException;
import io.confluent.connect.hdfs.filter.CommittedFileFilter;
import io.confluent.connect.hdfs.filter.TopicPartitionCommittedFileFilter;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.schema.Compatibility;
import io.confluent.connect.hdfs.schema.SchemaUtils;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.wal.WAL;

import org.apache.kafka.clients.producer.Producer;

public class TopicPartitionWriter {
  private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);
  private WAL wal;
  private Map<String, String> tempFiles;
  private Map<String, RecordWriter> writers;
  private TopicPartition tp;
  private Partitioner partitioner;
  private String url;
  private String topicsDir;
  private State state;
  private Queue<SinkRecord> buffer;
  private boolean recovered;
  private Storage storage;
  private SinkTaskContext context;
  private int recordCounter;
  private int flushSize;
  private long rotateIntervalMs;
  private long lastRotate;
  private RecordWriterProvider writerProvider;
  private Configuration conf;
  private AvroData avroData;
  private Set<String> appended;
  private long offset;
  private boolean sawInvalidOffset;
  private Map<String, Long> startOffsets;
  private Map<String, Long> offsets;
  private long timeoutMs;
  private long failureTime;
  private Compatibility compatibility;
  private Schema currentSchema;
  private HdfsSinkConnectorConfig connectorConfig;
  private String extension;
  private final String zeroPadOffsetFormat;

  private final boolean hiveIntegration;
  private String hiveDatabase;
  private HiveMetaStore hiveMetaStore;
  private SchemaFileReader schemaFileReader;
  private HiveUtil hive;
  private ExecutorService executorService;
  private Queue<Future> hiveUpdateFutures;
  private Set<String> hivePartitions;

  private Producer<String, Object> writerLogProducer;

  public TopicPartitionWriter(
      TopicPartition tp,
      Storage storage,
      RecordWriterProvider writerProvider,
      Partitioner partitioner,
      HdfsSinkConnectorConfig connectorConfig,
      SinkTaskContext context,
      AvroData avroData) {
    this(tp, storage, writerProvider, partitioner, connectorConfig, context, avroData, null, null, null, null, null, null);
  }

  public TopicPartitionWriter(
      TopicPartition tp,
      Storage storage,
      RecordWriterProvider writerProvider,
      Partitioner partitioner,
      HdfsSinkConnectorConfig connectorConfig,
      SinkTaskContext context,
      AvroData avroData,
      HiveMetaStore hiveMetaStore,
      HiveUtil hive,
      SchemaFileReader schemaFileReader,
      ExecutorService executorService,
      Queue<Future> hiveUpdateFutures,
      Producer<String, Object> writerLogProducer) {
    this.tp = tp;
    this.connectorConfig = connectorConfig;
    this.context = context;
    this.avroData = avroData;
    this.storage = storage;
    this.writerProvider = writerProvider;
    this.partitioner = partitioner;
    this.url = storage.url();
    this.conf = storage.conf();
    this.schemaFileReader = schemaFileReader;
    this.writerLogProducer = writerLogProducer;

    topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPICS_DIR_CONFIG);
    flushSize = connectorConfig.getInt(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG);
    rotateIntervalMs = connectorConfig.getLong(HdfsSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG);
    timeoutMs = connectorConfig.getLong(HdfsSinkConnectorConfig.RETRY_BACKOFF_CONFIG);
    compatibility = SchemaUtils.getCompatibility(
        connectorConfig.getString(HdfsSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG));

    String logsDir = connectorConfig.getString(HdfsSinkConnectorConfig.LOGS_DIR_CONFIG);
    wal = storage.wal(logsDir, tp);

    buffer = new LinkedList<>();
    writers = new HashMap<>();
    tempFiles = new HashMap<>();
    appended = new HashSet<>();
    startOffsets = new HashMap<>();
    offsets = new HashMap<>();
    state = State.RECOVERY_STARTED;
    failureTime = -1L;
    offset = -1L;
    sawInvalidOffset = false;
    extension = writerProvider.getExtension();
    zeroPadOffsetFormat
        = "%0" +
          connectorConfig.getInt(HdfsSinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG) +
          "d";

    hiveIntegration = connectorConfig.getBoolean(HdfsSinkConnectorConfig.HIVE_INTEGRATION_CONFIG);
    if (hiveIntegration) {
      hiveDatabase = connectorConfig.getString(HdfsSinkConnectorConfig.HIVE_DATABASE_CONFIG);
      this.hiveMetaStore = hiveMetaStore;
      this.hive = hive;
      this.executorService = executorService;
      this.hiveUpdateFutures = hiveUpdateFutures;
      hivePartitions = new HashSet<>();
    }
  }

  private enum State {
    RECOVERY_STARTED,
    RECOVERY_PARTITION_PAUSED,
    WAL_APPLIED,
    WAL_TRUNCATED,
    OFFSET_RESET,
    WRITE_STARTED,
    WRITE_PARTITION_PAUSED,
    SHOULD_ROTATE,
    TEMP_FILE_CLOSED,
    WAL_APPENDED,
    FILE_COMMITTED;

    private static State[] vals = values();
    public State next() {
      log.debug("===>>> [1] Next state is {}", (this.ordinal() + 1) % vals.length);
      return vals[(this.ordinal() + 1) % vals.length];
    }
  }

  public boolean recover() {
    try {
      switch (state) {
        case RECOVERY_STARTED:
          log.info("Started recovery for topic partition {}", tp);
          pause();
          nextState();
        case RECOVERY_PARTITION_PAUSED:
          applyWAL();
          nextState();
        case WAL_APPLIED:
          truncateWAL();
          nextState();
        case WAL_TRUNCATED:
          resetOffsets();
          nextState();
        case OFFSET_RESET:
          resume();
          nextState();
          log.info("Finished recovery for topic partition {}", tp);
          break;
        default:
          log.error("{} is not a valid state to perform recovery for topic partition {}.", state, tp);
      }
    } catch (ConnectException e) {
      log.error("Recovery failed at state {}", state, e);
      setRetryTimeout(timeoutMs);
      return false;
    }
    return true;
  }

  public void write() {
    long now = System.currentTimeMillis();
    if (failureTime > 0 && now - failureTime < timeoutMs) {
      return;
    }
    if (state.compareTo(State.WRITE_STARTED) < 0) {
      boolean success = recover();
      if (!success) {
        return;
      }
    }
    while(!buffer.isEmpty()) {
      try {
        switch (state) {
          case WRITE_STARTED:
            pause();
            nextState();
          case WRITE_PARTITION_PAUSED:
            if (currentSchema == null) {
              if (compatibility != Compatibility.NONE && offset != -1) {
                String topicDir = FileUtils.topicDirectory(url, topicsDir, tp.topic());
                CommittedFileFilter filter = new TopicPartitionCommittedFileFilter(tp);
                FileStatus fileStatusWithMaxOffset = FileUtils.fileStatusWithMaxOffset(storage, new Path(topicDir), filter);
                if (fileStatusWithMaxOffset != null) {
                  currentSchema = schemaFileReader.getSchema(conf, fileStatusWithMaxOffset.getPath());
                }
              }
            }
            SinkRecord record = buffer.peek();
            Schema valueSchema = record.valueSchema();
            if (SchemaUtils.shouldChangeSchema(valueSchema, currentSchema, compatibility)) {
              currentSchema = valueSchema;
              if (hiveIntegration) {
                createHiveTable();
                alterHiveSchema();
              }
              if (recordCounter > 0) {
                nextState();
              } else {
                break;
              }
            } else {
              SinkRecord projectedRecord = SchemaUtils.project(record, currentSchema, compatibility);
              writeRecord(projectedRecord);
              buffer.poll();
              if (shouldRotate(now)) {
                log.info("Starting commit and rotation for topic partition {} with start offsets {}"
                         + " and end offsets {}", tp, startOffsets, offsets);
                nextState();
                // Fall through and try to rotate immediately
              } else {
                log.debug("===>>> [8] shouldRotate is false");
                break;
              }
            }
          case SHOULD_ROTATE:
            lastRotate = System.currentTimeMillis();
            closeTempFile();
            nextState();
          case TEMP_FILE_CLOSED:
            appendToWAL();
            nextState();
          case WAL_APPENDED:
            commitFile();
            nextState();
          case FILE_COMMITTED:
            setState(State.WRITE_PARTITION_PAUSED);
            break;
          default:
            log.error("{} is not a valid state to write record for topic partition {}.", state, tp);
        }
      } catch (SchemaProjectorException | IllegalWorkerStateException | HiveMetaStoreException e ) {
        throw new RuntimeException(e);
      } catch (IOException | ConnectException e) {
        log.error("Exception on {}.", tp);
        failureTime = System.currentTimeMillis();
        setRetryTimeout(timeoutMs);
        break;
      }
    }
    if (buffer.isEmpty()) {
      resume();
      state = State.WRITE_STARTED;
    }
  }

  public void close() throws ConnectException {
    log.debug("Closing TopicPartitionWriter {}", tp);
    List<Exception> exceptions = new ArrayList<>();
    for (String encodedPartition : tempFiles.keySet()) {
      try {
        if (writers.containsKey(encodedPartition)) {
          log.debug("Discarding in progress tempfile {} for {} {}",
                    tempFiles.get(encodedPartition), tp, encodedPartition);
          closeTempFile(encodedPartition);
          deleteTempFile(encodedPartition);
        }
      } catch (IOException e) {
        log.error("Error discarding temp file {} for {} {} when closing TopicPartitionWriter:",
                  tempFiles.get(encodedPartition), tp, encodedPartition, e);
      }
    }

    writers.clear();

    try {
      wal.close();
    } catch (ConnectException e) {
      log.error("Error closing {}.", wal.getLogFile(), e);
      exceptions.add(e);
    }
    startOffsets.clear();
    offsets.clear();

    if (exceptions.size() != 0) {
      StringBuilder sb = new StringBuilder();
      for (Exception exception: exceptions) {
        sb.append(exception.getMessage());
        sb.append("\n");
      }
      throw new ConnectException("Error closing writer: " + sb.toString());
    }
  }

  public void buffer(SinkRecord sinkRecord) {
    buffer.add(sinkRecord);
  }

  public long offset() {
    return offset;
  }

  public Map<String, RecordWriter> getWriters() {
    return writers;
  }

  public Map<String, String> getTempFiles() {
    return tempFiles;
  }

  public String getExtension() {
    return writerProvider.getExtension();
  }

  private String getDirectory(String encodedPartition) {
    return partitioner.generatePartitionedPath(tp.topic(), encodedPartition);
  }

  private void nextState() {
    state = state.next();
  }

  private void setState(State state) {
    this.state = state;
  }

  private boolean shouldRotate(long now) {
    if (recordCounter >= flushSize) {
      return true;
    } else if (rotateIntervalMs <= 0) {
      return false;
    } else {
      return now - lastRotate >= rotateIntervalMs;
    }
  }

  private void readOffset() throws ConnectException {
    try {
      String path = FileUtils.topicDirectory(url, topicsDir, tp.topic());
      CommittedFileFilter filter = new TopicPartitionCommittedFileFilter(tp);
      FileStatus fileStatusWithMaxOffset = FileUtils.fileStatusWithMaxOffset(storage, new Path(path), filter);
      if (fileStatusWithMaxOffset != null) {
        offset = FileUtils.extractOffset(fileStatusWithMaxOffset.getPath().getName()) + 1;
      }
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  private void pause() {
    context.pause(tp);
  }

  private void resume() {
    context.resume(tp);
  }

  @SuppressWarnings("unchecked")
  private RecordWriter<SinkRecord> getWriter(SinkRecord record, String encodedPartition)
      throws ConnectException {
    try {
      log.debug("===>>> [14] record = {} getWriter = {}", record.toString(), writers.toString());
      if (writers.containsKey(encodedPartition)) {
        return writers.get(encodedPartition);
      }
      String tempFile = getTempFile(encodedPartition);
      RecordWriter<SinkRecord> writer = writerProvider.getRecordWriter(conf, tempFile, record, avroData);
      writers.put(encodedPartition, writer);
      log.debug("===>>> [15] Added new writer - partition {}, writer {}", encodedPartition, writer.toString());
      if (hiveIntegration && !hivePartitions.contains(encodedPartition)) {
        addHivePartition(encodedPartition);
        hivePartitions.add(encodedPartition);
      }
      log.debug("===>>> [18] record = {} getWriter {}", record.toString(), writer.toString());
      return writer;
    } catch (IOException e) {
      log.debug("===>>> [16] record = {} getWriter exception {}", record.toString(), e.getMessage());
      throw new ConnectException(e);
    }
  }

  private String getTempFile(String encodedPartition) {
    String tempFile;
    if (tempFiles.containsKey(encodedPartition)) {
      tempFile = tempFiles.get(encodedPartition);
      log.debug("===>>> [12] getTempFile {}", tempFile);
    } else {
      String directory = HdfsSinkConnecorConstants.TEMPFILE_DIRECTORY + getDirectory(encodedPartition);
      tempFile = FileUtils.tempFileName(url, topicsDir, directory, extension);
      log.debug("===>>> [13] getTempFile {}", tempFile);
      tempFiles.put(encodedPartition, tempFile);
    }
    return tempFile;
  }

  private void applyWAL() throws ConnectException {
    if (!recovered) {
      wal.apply();
    }
  }

  private void truncateWAL() throws ConnectException {
    if (!recovered) {
      wal.truncate();
    }
  }

  private void resetOffsets() throws ConnectException {
    if (!recovered) {
      readOffset();
      if (offset > 0) {
        log.debug("Resetting offset for {} to {}", tp, offset);
        context.offset(tp, offset);
      }
      recovered = true;
    }
  }

  private void writeRecord(SinkRecord record) throws IOException {
    long expectedOffset = offset + recordCounter;
    if (offset == -1) {
      offset = record.kafkaOffset();
    } else if (record.kafkaOffset() != expectedOffset) {
      // Currently it's possible to see stale data with the wrong offset after a rebalance when you
      // rewind, which we do since we manage our own offsets. See KAFKA-2894.
      if (!sawInvalidOffset) {
        log.info(
            "Ignoring stale out-of-order record in {}-{}. Has offset {} instead of expected offset {}",
            record.topic(), record.kafkaPartition(), record.kafkaOffset(), expectedOffset);
      }
      sawInvalidOffset = true;
      return;
    }

    if (sawInvalidOffset) {
      log.info(
          "Recovered from stale out-of-order records in {}-{} with offset {}",
          record.topic(), record.kafkaPartition(), expectedOffset);
      sawInvalidOffset = false;
    }

    String encodedPartition = partitioner.encodePartition(record);
    RecordWriter<SinkRecord> writer = getWriter(record, encodedPartition);
    log.debug("===>>> [2] Write a record {}, encodedPartition = {}", record.toString(), encodedPartition);
    writer.write(record);
    log.debug("===>>> [3] Write a record {} by the writer {}", record.toString(), writer.toString());

    if (!startOffsets.containsKey(encodedPartition)) {
      startOffsets.put(encodedPartition, record.kafkaOffset());
      offsets.put(encodedPartition, record.kafkaOffset());
    } else {
      offsets.put(encodedPartition, record.kafkaOffset());
    }
    recordCounter++;
  }

  private void closeTempFile(String encodedPartition) throws IOException {
    if (writers.containsKey(encodedPartition)) {
      RecordWriter writer = writers.get(encodedPartition);
      writer.close();
      writers.remove(encodedPartition);
    }
  }

  private void closeTempFile() throws IOException {
    for (String encodedPartition: tempFiles.keySet()) {
      closeTempFile(encodedPartition);
    }
  }

  private void appendToWAL(String encodedPartition) throws IOException {
    String tempFile = tempFiles.get(encodedPartition);
    if (appended.contains(tempFile)) {
      log.debug("===>>> [4] appended doesn't contain {}", tempFile);
      return;
    }
    if (!startOffsets.containsKey(encodedPartition)) {
      log.debug("===>>> [5] startOffsets doesn't contain {}", encodedPartition);
      return;
    }
    long startOffset = startOffsets.get(encodedPartition);
    long endOffset = offsets.get(encodedPartition);
    String directory = getDirectory(encodedPartition);
    String committedFile = FileUtils.committedFileName(url, topicsDir, directory, tp,
                                                       startOffset, endOffset, extension,
                                                       zeroPadOffsetFormat);
    wal.append(tempFile, committedFile);
    appended.add(tempFile);
    log.debug("===>>> [6] appendToWAL {}", tempFile);
  }

  private void appendToWAL() throws IOException {
    log.debug("===>>> [9] beginAppend {}", tempFiles.keySet().toString());
    beginAppend();
    for (String encodedPartition: tempFiles.keySet()) {
      appendToWAL(encodedPartition);
    }
    endAppend();
  }

  private void beginAppend() throws IOException {
    if (!appended.contains(WAL.beginMarker)) {
      wal.append(WAL.beginMarker, "");
    }
  }

  private void endAppend() throws IOException {
    if (!appended.contains(WAL.endMarker)) {
      wal.append(WAL.endMarker, "");
    }
  }

  private void commitFile() throws IOException {
    appended.clear();
    for (String encodedPartition: tempFiles.keySet()) {
      commitFile(encodedPartition);
    }
  }

  private ProducerRecord<String, Object> createLogRecord(TopicPartition tp, String dir, String file) {
    String topic;
    try {
      HashMap<String, String> valuesMap = new HashMap();
      valuesMap.put("connector", connectorConfig.getString(HdfsSinkConnectorConfig.CONNECTOR_NAME_CONFIG));
      valuesMap.put("topic", tp.topic());
      StrSubstitutor sub = new StrSubstitutor(valuesMap);
      topic = sub.replace(connectorConfig.getString(HdfsSinkConnectorConfig.WRITER_LOGGING_TOPIC_FORMAT_CONFIG));
    } catch (Exception e) {
      topic = "unknown_connector-" + tp.topic() + "-log";
    }

    String key = dir;
    org.apache.avro.Schema schema = org.apache.avro.SchemaBuilder.record("writerlog").fields()
            .requiredString("file").requiredLong("time")
            .endRecord();
    org.apache.avro.generic.GenericRecord value = new org.apache.avro.generic.GenericData.Record(schema);
    value.put("file", file);
    value.put("time",  Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis());

    return new ProducerRecord(topic, key, value);
  }

  private void commitFile(String encodedPartiton) throws IOException {
    if (!startOffsets.containsKey(encodedPartiton)) {
      log.debug("===>>> [7] !startOffsets.containsKey {}", encodedPartiton);
      return;
    }
    long startOffset = startOffsets.get(encodedPartiton);
    long endOffset = offsets.get(encodedPartiton);
    String tempFile = tempFiles.get(encodedPartiton);
    String directory = getDirectory(encodedPartiton);
    String committedFile = FileUtils.committedFileName(url, topicsDir, directory, tp,
                                                       startOffset, endOffset, extension,
                                                       zeroPadOffsetFormat);

    log.debug("===>>> [10] Before commitFile: {}", committedFile);
    String directoryName = FileUtils.directoryName(url, topicsDir, directory);
    if (!storage.exists(directoryName)) {
      storage.mkdirs(directoryName);
    }
//    if (writerLogProducer != null) {
//      try {
//        ProducerRecord data = createLogRecord(tp, directoryName, committedFile);
//        writerLogProducer.send(data).get();
//      } catch (Exception e) {
//        throw new IOException("Writer Logging failed: " + e.toString());
//      }
//    }
    log.debug("===>>> [11] Before commitFile: {} tempFile {}", committedFile, tempFile);
    storage.commit(tempFile, committedFile);
    startOffsets.remove(encodedPartiton);
    offset = offset + recordCounter;
    recordCounter = 0;
    log.debug("===>>> [12] After commitFile: {}", committedFile);
    //log.info("Committed {} for {}", committedFile, tp);
  }

  private void deleteTempFile(String encodedPartiton) throws IOException {
    storage.delete(tempFiles.get(encodedPartiton));
  }

  private void setRetryTimeout(long timeoutMs) {
    context.timeout(timeoutMs);
  }

  private void createHiveTable() {
    Future<Void> future = executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws HiveMetaStoreException {
        hive.createTable(hiveDatabase, tp.topic(), currentSchema, partitioner);
        return null;
      }
    });
    hiveUpdateFutures.add(future);
  }

  private void alterHiveSchema() {
    Future<Void> future = executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws HiveMetaStoreException {
        hive.alterSchema(hiveDatabase, tp.topic(), currentSchema);
        return null;
      }
    });
    hiveUpdateFutures.add(future);
  }

  private void addHivePartition(final String location) {
    Future<Void> future = executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        hiveMetaStore.addPartition(hiveDatabase, tp.topic(), location);
        return null;
      }
    });
    hiveUpdateFutures.add(future);
  }
}
