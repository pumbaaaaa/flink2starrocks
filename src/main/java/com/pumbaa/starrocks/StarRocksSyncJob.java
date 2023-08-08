package com.pumbaa.starrocks;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;


public class StarRocksSyncJob {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSyncJob.class);

    public static void main(String[] args) throws Exception {
        ParameterTool argsParam = ParameterTool.fromArgs(args);
        ParameterTool confParam = ParameterTool.fromPropertiesFile("src/main/resources/config-local.properties");
        String instanceHost = argsParam.get(Constants.INSTANCE_HOST);

        List<TableConf> tableConfList = getCdcConf(instanceHost, confParam);
        String[] dbList = tableConfList.stream().map(e -> e.getDbName()).toArray(size -> new String[size]);
        String[] tableList = tableConfList.stream().map(e -> e.getDbName() + "." + e.getSourceTable()).toArray(size -> new String[size]);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(instanceHost)
                .port(confParam.getInt(Constants.SOURCE_PORT))
                .scanNewlyAddedTableEnabled(true)
                .databaseList(dbList)
                .tableList(tableList)
                .username(Constants.SOURCE_USER)
                .password(Constants.SOURCE_PWD)
                .serverTimeZone(TimeZone.getDefault().toString())
                .deserializer(new StarRocksDeserializetionSchema()).build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);

        for (TableConf tableConf : tableConfList) {
            String odsTablePrefix = tableConf.getOdsTablePrefix();
            String sourceTable = tableConf.getSourceTable();
            String filterUid = tableConf.getFilterUid();
            String flatmapUid = tableConf.getFlatmapUid();
            String sinkUid = tableConf.getSinkUid();
            String sourceUid = tableConf.getSourceUid();

            SingleOutputStreamOperator<String> cdcSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source").uid(sourceUid);
            SingleOutputStreamOperator filterStream = fifterTableData(cdcSource, sourceTable, filterUid);
            SingleOutputStreamOperator<String> cleanStream = setStreamLoadOp(filterStream, flatmapUid);

            String targetTable = StringUtils.isBlank(odsTablePrefix) ? sourceTable : odsTablePrefix + "_" + sourceTable;
            SinkFunction starRocksSink = buildStarRocksSink(targetTable, confParam);
            cleanStream.addSink(starRocksSink).uid(sinkUid).name(targetTable);
        }

        env.execute("Mysql Sync StarRocks");

    }

    public static SinkFunction buildStarRocksSink(String table, ParameterTool confParam) {
        return StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://" + confParam.get(Constants.TARGET_HOST) + ":" + confParam.get(Constants.TARGET_PORT))
                        .withProperty("load-url", confParam.get(Constants.TARGET_HOST) + ":" + confParam.get(Constants.TARGET_HTTP_PORT))
                        .withProperty("username", confParam.get(Constants.TARGET_USER))
                        .withProperty("password", confParam.get(Constants.TARGET_PWD))
                        .withProperty("table-name", table)
                        .withProperty("database-name", confParam.get(Constants.TARGET_DB))
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        .withProperty("sink.buffer-flush.interval-ms", "5000")
                        .withProperty("sink.buffer-flush.max-rows", "64000")
                        .withProperty("sink.max-retries", "5").build()
        );
    }

    public static SingleOutputStreamOperator fifterTableData(SingleOutputStreamOperator<String> cdcSource, String table, String filterUid) {
        return cdcSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String row) throws Exception {
                try {
                    JSONObject rowJson = JSON.parseObject(row);
                    String tbl = rowJson.getString("table");
                    return table.equals(tbl);
                } catch (Exception ex) {
                    LOG.error(ex.getStackTrace().toString());
                    return false;
                }
            }
        }).uid(filterUid).name("Filter");
    }

    public static SingleOutputStreamOperator<String> setStreamLoadOp(SingleOutputStreamOperator source, String flatmapUid) {
        return source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String row, Collector<String> out) throws Exception {
                try {
                    JSONObject rowJson = JSON.parseObject(row);
                    String op = rowJson.getString(Envelope.FieldName.OPERATION);
                    if (Arrays.asList(Envelope.Operation.CREATE.code(), Envelope.Operation.READ.code(), Envelope.Operation.UPDATE.code()).contains(op)) {
                        JSONObject after = rowJson.getJSONObject(Envelope.FieldName.AFTER);
                        after.put(StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.UPSERT.ordinal());
                        out.collect(after.toJSONString());
                    } else if (Envelope.Operation.DELETE.code().equals(op)) {
                        JSONObject before = rowJson.getJSONObject(Envelope.FieldName.BEFORE);
                        before.put(StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.DELETE.ordinal());
                        out.collect(before.toJSONString());
                    } else {
                        LOG.info("filter other op:{}", op);
                    }
                } catch (Exception e) {
                    LOG.error(e.getStackTrace().toString());
                    LOG.error("filter other format binlog:{} ", row);
                }
            }
        }).uid(flatmapUid).name("FlatMap");
    }


    public static List<TableConf> getCdcConf(String instanceHost, ParameterTool confParam) {
        List<TableConf> result = new ArrayList<>();
        String sql = String.format("SELECT * FROM {} WHERE instance_ip = '{}'", confParam.get(Constants.CONFIG_TABLE), instanceHost);
        List<JSONObject> jsonObjectList = JdbcUtil.executeQuery(confParam.get(Constants.TARGET_HOST),
                confParam.getInt(Constants.TARGET_PORT),
                confParam.get(Constants.TARGET_USER),
                confParam.get(Constants.TARGET_PWD),
                sql);
        if (CollectionUtils.isNotEmpty(jsonObjectList)) {

            for (JSONObject jsonObject : jsonObjectList) {
                TableConf tableConf = new TableConf();
                tableConf.setInstanceIp(jsonObject.getString(Constants.INSTANCE_HOST));
                tableConf.setDbName(jsonObject.getString(Constants.DB_NAME));
                tableConf.setServerId(jsonObject.getString(Constants.SERVER_ID));
                tableConf.setSourceTable(jsonObject.getString(Constants.SOURCE_TABLE));
                tableConf.setOdsTablePrefix(jsonObject.getString(Constants.ODS_TABLE_PREFIX));
                tableConf.setSourceUid(jsonObject.getString(Constants.SOURCE_UID));
                tableConf.setFilterUid(jsonObject.getString(Constants.FILTER_UID));
                tableConf.setFlatmapUid(jsonObject.getString(Constants.FLATMAP_UID));
                tableConf.setSinkUid(jsonObject.getString(Constants.SINK_UID));
                result.add(tableConf);
            }
        }
        return result;
    }

}
