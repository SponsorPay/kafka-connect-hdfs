package io.confluent.connect.hdfs;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class NotificationTests extends TestWithMiniDFSCluster {
    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(HdfsSinkConnectorConfig.CONNECTOR_NAME_CONFIG, "test_connector");
        props.put(HdfsSinkConnectorConfig.WRITER_LOGGING_CONFIG, "true");
        props.put(HdfsSinkConnectorConfig.WRITER_LOGGING_BROKERS_CONFIG, "xray01.local0:9092,xray02.local0:9092");
        props.put(HdfsSinkConnectorConfig.WRITER_LOGGING_SCHEMA_REGISTRY_CONFIG, "http://xray01.local0:8081");
        return props;
    }

    @Test
    public void testCommitNotification() throws Exception {
        DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
        hdfsWriter.recover(TOPIC_TEST_PARTITION);

        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = 0; offset < 7; offset++) {
            SinkRecord sinkRecord =
                    new SinkRecord(TOPIC_TEST, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset);

            sinkRecords.add(sinkRecord);
        }
        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
    }
}
