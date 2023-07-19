package com.qingxuan.flink.cdc.sql;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * mysql cdc sql
 * */
public class MysqlCDC4Sql {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(5000L);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //创建 Flink-MySQL-CDC 的 Source
        tableEnvironment.executeSql("CREATE TABLE student_flink (" +
                " id INT," +
                " name STRING," +
                " age INT" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = '10.211.55.14'," +
                " 'port' = '32306'," +
                " 'username' = 'root'," +
                " 'password' = 'root'," +
                " 'database-name' = 'flink'," +
                //时区
                " 'server-time-zone' = 'UTC'," +
                " 'table-name' = 'student_sql'," +
                " 'scan.incremental.snapshot.enabled' = 'true'," +
                " 'scan.incremental.snapshot.chunk.key-column' = 'id'," +
                //initial全量,如果想后续增量,则需要enableCheckpointing, latest-offset增量
                        " 'scan.startup.mode' = 'initial'" +

                ")");

        // 3.执行sql，并打印
        tableEnvironment.executeSql("select * from student_flink").print();
        // 4.执行任务
        environment.execute();
    }
}
