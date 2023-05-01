package com.qingxuan.flink.datastream;

import com.qingxuan.flink.SocketWindowWordCount;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.atomic.AtomicLong;

public class DataStreamApiTest {

    @SneakyThrows
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 网站点击 Click 的数据流

        // get input data by connecting to the socket
        DataStream<String> clicks = env.socketTextStream("127.0.0.1", 9000, "\n");

        SingleOutputStreamOperator<Click> clickSingleOutputStreamOperator = clicks.flatMap(
                (FlatMapFunction<String, Click>)
                        (value, out) -> {
                            String[] split = value.split(",");
                            if (split.length == 2) {
                                Click click = new Click();
                                click.userId = split[0];
                                click.articleId = split[1];
                                out.collect(click);
                            }
                        },
                Types.POJO(Click.class));

        DataStream<Tuple2<String, Long>> result = clickSingleOutputStreamOperator
                // 将网站点击映射为 (userId, 1) 以便计数
                .map(
                        // 实现 MapFunction 接口定义函数
                        new MapFunction<Click, Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> map(Click value) throws Exception {
                                return Tuple2.of(value.userId, 1L);
                            }
                        })
                // 以 userId 作为 key
                .keyBy(value->value.f0)
                // 定义超时的会话窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 对每个会话窗口的点击进行计数，使用 lambda 表达式定义 reduce 函数
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .returns(new TypeHint<Tuple2<String, Long>>() {});
        result.print().setParallelism(1);

        DataStream<Tuple2<String, Long>> result2 = clickSingleOutputStreamOperator
                // 将网站文章点击映射为 (userId, 1) 以便计数
                .map(
                        // 实现 MapFunction 接口定义函数
                        new MapFunction<Click, Tuple2<String,Long>>() {
                            @Override
                            public Tuple2<String,Long> map(Click value) throws Exception {
                                return Tuple2.of(value.articleId, 1L);
                            }
                        })
                // 以 文章id 作为 key
                .keyBy((KeySelector<Tuple2<String, Long>, Object>) value -> value.f0)
                // 定义 30 分钟超时的会话窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //.window(EventTimeSessionWindows.withGap(Time.seconds(10L)))
                // 对每个会话窗口的点击进行计数，使用 lambda 表达式定义 reduce 函数
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1));

        result2.print().setParallelism(1);



        DataStream<Tuple2<String,Long>> result3 = clickSingleOutputStreamOperator
                // 将网站文章点击映射为 (userId, 1) 以便计数
                .map(
                        // 实现 MapFunction 接口定义函数
                        new MapFunction<Click, Tuple2<String,Long>>() {
                            @Override
                            public Tuple2<String,Long> map(Click value) throws Exception {
                                return Tuple2.of("全部",1L);
                            }
                        })
                .keyBy(0)
                // 定义 30 分钟超时的会话窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //.window(EventTimeSessionWindows.withGap(Time.seconds(10L)))
                // 对每个会话窗口的点击进行计数，使用 lambda 表达式定义 reduce 函数
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1));
        result3.print().setParallelism(1);


        env.execute("user");
    }

    @Data
    public static class Click {
        private String userId;
        private String articleId;

        @Override
        public String toString() {
            return userId + " : " + articleId;
        }
    }
}
