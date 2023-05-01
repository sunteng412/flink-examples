package com.qingxuan.flink.example.spendreport;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.expressions.TimeIntervalUnit;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 实时金融大屏
 *
 * @author : longchuan
 * @version :
 * @date : 2023/3/6 7:12 下午
 * @description:
 */
@Slf4j
public class SpendReport {


//    public static Table report(Table transactions) {
//        //按照账户显示一天中每个小时的总支出
//        return transactions.select(
//                $("account_id"),
//                //毫秒粒度的时间戳字段需要向下舍入到小时
//                //$("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
//                //call(MyFloor.class, $("transaction_time")).as("log_ts"),
//                $("amount"))
//                .groupBy($("account_id"), $("log_ts"))
//                .select(
//                        $("account_id"),
//                        $("log_ts"),
//                        $("amount").sum().as("amount"));
//    }

    public static Table report(Table transactions) {
        return transactions
                .window(Tumble.over(lit(1).hour()).on($("transaction_time")).as("log_ts"))
                .groupBy($("account_id"), $("log_ts"))
                .select(
                        $("account_id"),
                        $("log_ts").start().as("log_ts"),
                        $("amount").sum().as("amount"));
    }

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        try {
            log.info("classloader:{},{}",Class.forName("com.mysql.cj.jdbc.Driver").getClassLoader().toString(),
                    Thread.currentThread().getContextClassLoader().toString());
            // Create a HiveCatalog
            MySqlCatalog catalog = new MySqlCatalog(Class.forName("com.mysql.cj.jdbc.Driver").getClassLoader()
                    , "mysql_catalog", "catalog_db","root","111111",
                    "jdbc:mysql://127.0.0.1:3306");
            catalog.supportsManagedTable();

            // Register the catalog
            tableEnvironment.registerCatalog(catalog.getName(), catalog);
        } catch (Exception e){
            log.error("classloader:{},catalog error:{}",Thread.currentThread().getContextClassLoader().toString(), Throwables.getStackTraceAsString(e));
        }



        //tableEnvironment.useCatalog("mysql_catalog");

        tableEnvironment.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'http',\n" +
                "    'url' = '127.0.0.1:8080/output',\n" +
                "    'type' = 'GET',\n" +
                "    'format'    = 'json'\n" +
                ")");




        tableEnvironment.executeSql("CREATE TABLE spend_report (\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     TIMESTAMP,\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "    'connector' = 'http',\n" +
                "    'type' = 'POST',\n" +
                "    'url' = '127.0.0.1:8080/input',\n" +
                "    'type' = 'POST',\n" +
                "    'format'    = 'json'\n" +
                ")");

        Table transactions = tableEnvironment.from("transactions");
        report(transactions).executeInsert("spend_report");
    }

}
