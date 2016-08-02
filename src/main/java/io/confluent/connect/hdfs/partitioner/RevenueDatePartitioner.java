package io.confluent.connect.hdfs.partitioner;

import io.confluent.common.config.ConfigException;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.PartitionException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RevenueDatePartitioner implements Partitioner {
    private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);
    private static String pathFormat = "'year'=YYYY/'month'=MM/'day'=dd/";
    private  DateTimeFormatter eventFormatter;

    // Duration of a partition in milliseconds.
    private DateTimeFormatter formatter;
    protected List<FieldSchema> partitionFields = new ArrayList<>();
    private static String patternString = "'year'=Y{1,5}/('month'=M{1,5}/)?('day'=d{1,3}/)?";
    private static Pattern pattern = Pattern.compile(patternString);

    protected void init(String pathFormat, Locale locale,
                        DateTimeZone timeZone, boolean hiveIntegration) {
        this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
        addToPartitionFields(pathFormat, hiveIntegration);
    }

    private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
        return DateTimeFormat.forPattern(str).withZone(timeZone);
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
        eventFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(timeZone);
        init(pathFormat, locale, timeZone, hiveIntegration);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        String record;
        String fieldName = "revenue_date";
        Object value = sinkRecord.value();
        Schema valueSchema = sinkRecord.valueSchema();

        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            Object revenue_date = struct.get(fieldName);
            Schema.Type type = valueSchema.field(fieldName).schema().type();
            switch (type) {
                case STRING:
                    record = (String) revenue_date;
                    break;
                default:
                    log.error("Type {} is not supported as creation_type.", type.getName());
                    throw new PartitionException("Error encoding partition.");
            }
        } else {
            log.error("Value is not Struct type.");
            throw new PartitionException("Error encoding partition.");
        }

        LocalDateTime dateTime = LocalDateTime.parse(record, eventFormatter);

        return dateTime.toString(formatter);
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
        for (String field : pathFormat.split("/")) {
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
