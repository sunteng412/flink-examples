package com.qingxuan.flink.processfunction;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.sink.AlertSink;

/**
 * 将相邻的 keyed START 和 END 事件相匹配并计算两者的时间间隔
 * 输入数据为 Tuple2<String, String> 类型，第一个字段为 key 值，
 * 第二个字段标记 START 和 END 事件。
 *
 * @author     : longchuan
 * @date       : 2023/2/24 2:36 下午
 * @description:
 * @version    : 
 */
public class StartEndDurationTest {
    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            //搞个默认的端口
            port = params.has("port") ? params.getInt("port") : 9000;
        } catch (Exception e) {
            System.err.println(
                    "No port specified. Please run 'StartEndDurationTest "
                            + "--hostname <hostname> --port <port>', where hostname (localhost by default) "
                            + "and port is the address of the text server");
            System.err.println(
                    "To start a simple text server, run 'netcat -l <port>' and "
                            + "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");
        text.flatMap(new FlatMapFunction<String, Tuple2<String,String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                String[] split = value.split(",");
                if(split.length == 2){
                    out.collect(Tuple2.of(split[0],split[1]));
                } else {
                    out.collect(Tuple2.of("-1",split[0]));
                }
            }
        }).keyBy((KeySelector<Tuple2<String, String>, String>) value -> value.f0)
                .process(new StartEndDuration()).addSink(new PrintSinkFunction());

        env.execute("StartEndDuration");
    }
    
}
