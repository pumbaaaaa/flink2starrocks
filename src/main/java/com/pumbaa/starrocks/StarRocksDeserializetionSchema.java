package com.pumbaa.starrocks;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;

public class StarRocksDeserializetionSchema implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = -8134462477010599298L;

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();

        Struct value = (Struct) sourceRecord.value();
        Struct sourceStruct = value.getStruct("source");

        result.put("op", Envelope.operationFor(sourceRecord).code());
        result.put("transaction", value.getStruct("transaction"));
        result.put("before", convertDateTime(value.getStruct("before")));
        result.put("after", convertDateTime(value.getStruct("after")));
        result.put("after", convertDateTime(value.getStruct("after")));
        result.put("table", sourceStruct.getString("table"));
        result.put("db", sourceStruct.getString("db"));

        collector.collect(result.toJSONString());
    }

    /**
     * 处理Datetime及timestamp字段时区问题
     *
     * @param struct
     * @return
     * @throws ParseException
     */
    private JSONObject convertDateTime(Struct struct) throws ParseException {
        JSONObject result = new JSONObject();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        sdf1.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        sdf2.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

        if (Objects.nonNull(struct)) {
            Schema schema = struct.schema();
            List<Field> fields = schema.fields();

            for (Field field : fields) {
                Object val = struct.get(field);
                String type = field.schema().type().getName();
                String name = field.schema().name();
                if ("int64".equals(type) && "io.debezium.time.Timestamp".equals(name)) {
                    if (val != null) {
                        long times = (long) val;
                        String dateTime = sdf.format(new Date((times - 8 * 60 * 60 * 1000)));
                        result.put(field.name(), dateTime);
                    }
                } else if ("int64".equals(type) && "io.debezium.time.MicroTimestamp".equals(name)) {
                    if (val != null) {
                        long times = (long) val / 1000;
                        String dateTime = sdf.format(new Date((times - 8 * 60 * 60 * 1000)));
                        result.put(field.name(), dateTime);
                    }
                } else if ("int64".equals(type) && "io.debezium.time.NanoTimestamp".equals(name)) {
                    if (val != null) {
                        long times = (long) val / 1000000;
                        String dateTime = sdf.format(new Date((times - 8 * 60 * 60 * 1000)));
                        result.put(field.name(), dateTime);
                    }
                } else if ("int32".equals(type) && "io.debezium.time.Date".equals(name)) {
                    if (val != null) {
                        int times = (int) val;
                        String dateTime = sdf1.format(new Date(times * 24 * 60 * 60L * 1000));
                        result.put(field.name(), dateTime);
                    }
                } else if ("string".equals(type) && "io.debezium.time.ZonedTimestamp".equals(name)) {
                    if (val != null) {
                        long newTime = sdf2.parse(val.toString()).getTime() + 8 * 60 * 60 * 1000;
                        String dateTime = sdf.format(new Date(newTime));
                        result.put(field.name(), dateTime);
                    }
                } else {
                    result.put(field.name(), val);
                }
            }
        }
        return result;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
