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

package io.confluent.connect.hdfs.partitioner;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class RevenueDatePartitionerTest {

    private static final long partitionDurationMs = TimeUnit.HOURS.toMillis(1);

    @Test
    public void testPartitionedPath() throws Exception {
        Map<String, Object> config = createConfig();

        RevenueDatePartitioner partitioner = new RevenueDatePartitioner();
        partitioner.configure(config);

        String pathFormat = partitioner.getPathFormat();
        String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
        long timestamp = new DateTime(2016, 4, 6, 14, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat,
                timeZoneString, timestamp);
        String path = partitioner.generatePartitionedPath("topic", encodedPartition);
        assertEquals("topic/year=2016/month=04/day=06/", path);
    }

    @Test
    public void testEncodedPartition() throws Exception {
        Map<String, Object> config = createConfig();
        String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
        String revenue_date = "2016-02-15";

        RevenueDatePartitioner partitioner = new RevenueDatePartitioner();
        partitioner.configure(config);
        Schema valueSchema = SchemaBuilder.struct()
          .name("Test schema").version(1).doc("Schema to test encoding")
          .field("revenue_date", Schema.STRING_SCHEMA)
          .build();
        Object value = new Struct(valueSchema).put("revenue_date", revenue_date);
        SinkRecord sinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", valueSchema, value, 0);

        String encodedPartition = partitioner.encodePartition(sinkRecord);
        assertEquals("year=2016/month=02/day=15/", encodedPartition);
    }

    @Test
    public void testFieldSchema() throws Exception {
        Map<String, Object> config = createConfig();

        RevenueDatePartitioner partitioner = new RevenueDatePartitioner();
        partitioner.configure(config);

        List<FieldSchema> fields = partitioner.partitionFields();
        assertEquals(fields.size(), 3);
        for (FieldSchema schema: fields) {
            if (schema.getName().equalsIgnoreCase("year")) {
                assertEquals("smallint", schema.getType());
            } else {
                assertEquals("tinyint", schema.getType());
            }
        }
    }

    private Map<String, Object> createConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(HdfsSinkConnectorConfig.LOCALE_CONFIG, "en");
        config.put(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, "UTC");
        return config;
    }
}