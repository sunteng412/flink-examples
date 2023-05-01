package com.qingxuan.flink.processfunction;
 
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.CompletableFuture;

public class BroadcastTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // TODO 2.读取数据构建数据源
        // 2.1读取数据构建广播流
        DataStreamSource<String> broadcastDs = env.socketTextStream("127.0.0.1", 9999);
        // 定义数据广播规则
        MapStateDescriptor<String, String> configFilter = new MapStateDescriptor<String, String>("configFilter", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        // 对数据进行广播
        BroadcastStream<String> broadcastConfig = broadcastDs.setParallelism(1).broadcast(configFilter);

        // 2.2读取数据构建常规流
        DataStreamSource<String> socketTextStream1 = env.socketTextStream("127.0.0.1", 8888);

        DataStream<String> resultDs = socketTextStream1.connect(broadcastConfig).process(new BroadcastProcessFunction<String, String, String>() {
            //设置拦截的关键字
            private String keyWords = null;
 
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                keyWords = "java";
                System.out.println("初始化keyWords: " + keyWords);
            }
 
            //每条都会处理
            @Override
            public void processElement(String value, ReadOnlyContext readOnlyContext, Collector<String> out) throws Exception {
                CompletableFuture.runAsync(()->{
                    if (value.contains(keyWords)) {
                        out.collect("触发短句:" + value + ", 关键字：" + keyWords);
                    }
                });
            }
 
            //对广播变量获取更新
            @Override
            public void processBroadcastElement(String value, Context context, Collector<String> out) throws Exception {
                keyWords = value;
                System.out.println("更新关键字:" + value);
            }
        });
 
        // 打印测试
        resultDs.print("result");
        env.execute("BroadcastTest");
    }
}