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

package com.qingxuan.flink;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.qingxuan.flink.example.custom.sink.LogSinkFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
public class SocketWindowWordCountSloatSharding {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

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
        },Types.TUPLE(Types.STRING, Types.INT))
                //.setParallelism(1)
                       // .slotSharingGroup("flatMap_sg")
                .keyBy(0)
                .countWindow(5)
                //tuple第1位，指定数量相加
                .sum(1).print();
                        //.setParallelism(4)executionEnvironment.execute("WindowWordCount");
                //        .slotSharingGroup("sum_sg")
        //forward.print().setParallelism(1);

        executionEnvironment.execute("WindowWordCount");
    }
}