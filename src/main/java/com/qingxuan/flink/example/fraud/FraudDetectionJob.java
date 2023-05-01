package com.qingxuan.flink.example.fraud;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * 欺诈监测
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/try-flink/datastream/#%E5%9C%A8-ide-%E4%B8%AD%E8%BF%90%E8%A1%8C
 *
 *
 * @author     : longchuan
 * @date       : 2023/3/2 7:15 下午
 * @description:
 * @version    :
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");

        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts
                .addSink(new AlertSink())
                .name("send-alerts");

        env.execute("Fraud Detection");
    }
}
