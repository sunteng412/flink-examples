package com.qingxuan.flink.datastream;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;

import java.util.ArrayList;

/**
 * dataStream简单案例
 * @author     : longchuan
 * @date       : 2023/3/27 7:48 下午
 * @description:
 * @version    :
 */
public class DataStreamApiSimpleTest {

    @SneakyThrows
    public static void main(String[] args) {

        ArrayList<Integer> list = Lists.newArrayList(1, 2, 3,5,6,7,8,9);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> streamSource = executionEnvironment.addSource(new FromElementsFunction<>(
                Types.INT.createSerializer(executionEnvironment.getConfig()), list), Types.INT);


        SingleOutputStreamOperator<Integer> streamOperator =
                streamSource.map(v -> v * 2).keyBy(value -> 1)
                        .sum(0);
        streamOperator.addSink(new PrintSinkFunction<>());
        streamOperator.setParallelism(1);
        executionEnvironment.execute();
    }

}
