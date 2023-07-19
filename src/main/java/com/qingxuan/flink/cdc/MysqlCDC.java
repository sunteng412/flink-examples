package com.qingxuan.flink.cdc;

import com.google.common.collect.Maps;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static com.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;

public class MysqlCDC {
    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.211.55.14")
                .port(32306)
                //允许ddl
                .includeSchemaChanges(Boolean.TRUE)
                //.startupOptions(StartupOptions.initial()) //全量同步
                .scanNewlyAddedTableEnabled(true) // 开启支持新增表
                .databaseList("flink") // set captured database
                .tableList("flink.student") // set captured table
                .username("root")
                .password("root")
                .debeziumProperties(getDebeziumProperties(Maps.newHashMap()))
                //.serverTimeZone("Asia/Shanghai")
                .serverTimeZone("UTC")
                .startupOptions(StartupOptions.earliest())
                //.deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String

                .deserializer(new CustomerMysqlDebeziumDeserialization())
                .build();

        Configuration configuration = new Configuration();
        // 生产环境夏下，改成参数传进来
//        configuration.setString("execution.savepoint.path","file:///tmp/flink-ck/1980d53f557a886f885172bcdf4be8e8/chk-21");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // enable checkpoint
        //env.enableCheckpointing(3000);
        // 设置本地
        //env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-ck");
        env
                .fromSource(mySqlSource,
                        //WatermarkStrategy.noWatermarks(),
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.of(1, ChronoUnit.SECONDS)),
                        "MySQL-Source")
                .setParallelism(1).addSink(new MysqlSink()).setParallelism(1);
                //.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot");
    }
}
