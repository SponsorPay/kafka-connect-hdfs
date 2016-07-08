package io.confluent.connect.hdfs.partitioner;

import io.confluent.connect.hdfs.errors.PartitionException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.confluent.common.config.ConfigException;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreationTimePartitioner implements Partitioner {
    private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);
    private static long partitionDurationMs = TimeUnit.HOURS.toMillis(1);
    private static String pathFormat = "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/";

    // Duration of a partition in milliseconds.
    private DateTimeFormatter formatter;
    protected List<FieldSchema> partitionFields = new ArrayList<>();
    private static String patternString = "'year'=Y{1,5}/('month'=M{1,5}/)?('day'=d{1,3}/)?('hour'=H{1,3}/)?('minute'=m{1,3}/)?";
    private static Pattern pattern = Pattern.compile(patternString);

    protected void init(String pathFormat, Locale locale,
                        DateTimeZone timeZone, boolean hiveIntegration) {
        this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
        addToPartitionFields(pathFormat, hiveIntegration);
    }

    private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
        return DateTimeFormat.forPattern(str).withZone(timeZone);
    }

    public static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {
        long adjustedTimeStamp = timeZone.convertUTCToLocal(timestamp);
        long partitionedTime = (adjustedTimeStamp / timeGranularityMs) * timeGranularityMs;
        return timeZone.convertLocalToUTC(partitionedTime, false);
    }

    @Override
    public void configure(Map<String, Object> config) {
        String localeString = (String) config.get(HdfsSinkConnectorConfig.LOCALE_CONFIG);
        if (localeString.equals("")) {
            throw new ConfigException(HdfsSinkConnectorConfig.LOCALE_CONFIG,
                    localeString, "Locale cannot be empty.");
        }
        String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
        if (timeZoneString.equals("")) {
            throw new ConfigException(HdfsSinkConnectorConfig.TIMEZONE_CONFIG,
                    timeZoneString, "Timezone cannot be empty.");
        }
        String hiveIntString = (String) config.get(HdfsSinkConnectorConfig.HIVE_INTEGRATION_CONFIG);
        boolean hiveIntegration = hiveIntString != null && hiveIntString.toLowerCase().equals("true");
        Locale locale = new Locale(localeString);
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
        init(pathFormat, locale, timeZone, hiveIntegration);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        long timestamp;
        String fieldName = "creation_timestamp";
        Object value = sinkRecord.value();
        Schema valueSchema = sinkRecord.valueSchema();
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            Object creationTime = struct.get(fieldName);
            Schema.Type type = valueSchema.field(fieldName).schema().type();
            switch (type) {
                case INT32:
                case INT64:
                    Number record = (Number) creationTime;
                    timestamp = record.longValue();
                    break;
                default:
                    log.error("Type {} is not supported as creation_type.", type.getName());
                    throw new PartitionException("Error encoding partition.");
            }
        } else {
            log.error("Value is not Struct type.");
            throw new PartitionException("Error encoding partition.");
        }
        DateTime bucket = new DateTime(getPartition(partitionDurationMs, timestamp, formatter.getZone()));
        return bucket.toString(formatter);
    }


    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
        return topic + "/" + encodedPartition;
    }

    @Override
    public List<FieldSchema> partitionFields() {
        return partitionFields;
    }

    private boolean verifyDateTimeFormat(String pathFormat) {
        Matcher m = pattern.matcher(pathFormat);
        return m.matches();
    }

    private void addToPartitionFields(String pathFormat, boolean hiveIntegration) {
        if (hiveIntegration && !verifyDateTimeFormat(pathFormat)) {
            throw new ConfigException(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG, pathFormat,
                    "Path format doesn't meet the requirements for Hive integration, "
                            + "which require prefixing each DateTime component with its name.");
        }
        for (String field: pathFormat.split("/")) {
            String[] parts = field.split("=");
            String fieldName = parts[0].replace("'", "");
            PrimitiveTypeInfo fieldType = fieldName.equalsIgnoreCase("year") ?
                    TypeInfoFactory.shortTypeInfo : TypeInfoFactory.byteTypeInfo;
            FieldSchema fieldSchema = new FieldSchema(fieldName, fieldType.toString(), "");
            partitionFields.add(fieldSchema);
        }
    }

    public String getPathFormat() {
        return pathFormat;
    }
}
