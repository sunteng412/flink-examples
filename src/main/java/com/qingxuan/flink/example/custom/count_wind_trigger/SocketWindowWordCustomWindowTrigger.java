/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qingxuan.flink.example.custom.count_wind_trigger;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.DynamicProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * countWindow 滑动窗口
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * <p>This program connects to a server socket and reads strings from the socket. The easiest way to
 * try this out is to open a text server (at port 12345) using the <i>netcat</i> tool via
 *
 * <pre>
 * nc -l 12345 on Linux or nc -l -p 12345 on Windows
 * </pre>
 *
 * <p>and run this example with the hostname and the port as arguments.
 */
@Slf4j
public class SocketWindowWordCustomWindowTrigger {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> list = Lists.newArrayList(
                "211111,1","211111,1","211111,1",
                "211111,1","211111,1","211111,1",
                "2,1", "1,1", "1,1", "7,1", "1,1", "1,1",
                "2,1", "1,1", "1,1", "7,1", "1,1", "1,1");

        DataStream<String> forward = executionEnvironment.addSource(new FromElementsFunction<>(
                Types.STRING.createSerializer(executionEnvironment.getConfig()), list), Types.STRING);

        //executionEnvironment.getConfig().setGlobalJobParameters(parameterTool);

        forward.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    List<String> split = Splitter.on(",").splitToList(value);
                    out.collect(Tuple2.of(split.get(0), 1));
                },Types.TUPLE(Types.STRING, Types.INT)).setParallelism(1)
                // .slotSharingGroup("flatMap_sg")
                .keyBy(0)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .trigger(new CustomCountWindTrigger<>(3, TimeCharacteristic.ProcessingTime))
                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String,Integer>, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String,Integer>,
                            TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> iterable,
                                        Collector<Tuple2<String,Integer>> collector) throws Exception {
                        Map<String, AtomicInteger> count = new HashMap<>();
                        for (Tuple2<String, Integer> val : iterable) {
                            count.compute(val.f0, (s, atomicInteger) -> {
                                if(Objects.isNull(atomicInteger)){
                                    atomicInteger=new AtomicInteger();
                                }
                                atomicInteger.incrementAndGet();
                                return atomicInteger;
                            });
                        }

                        count.forEach((k,v)->{
                            collector.collect(Tuple2.of(k,v.get()));
                        });

                    }
                })
                //tuple第1位 相加
                .print().setParallelism(2);


        executionEnvironment.execute("WindowWordCount");
    }
}