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

package com.qingxuan.flink.example.checkpoint;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * checkpoint 测试
 * @author     : longchuan
 * @date       : 2023/3/22 2:03 下午
 * @description:
 * @version    :
 */
@Slf4j
public class SocketWindowWordCountCheckPointTest {

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
                    "No port specified. Please run 'SocketWindowWordCountCheckPointTest "
                            + "--hostname <hostname> --port <port>', where hostname (localhost by default) "
                            + "and port is the address of the text server");
            System.err.println(
                    "To start a simple text server, run 'netcat -l <port>' and "
                            + "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //每隔10秒备份一次 模式为"EXACTLY_ONCE"
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        //备份最大并行度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/admin/work/flink/flink-1.16.1/qingxuan/checkpoint");
        //错误恢复策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,
                org.apache.flink.api.common.time.Time.seconds(10)));

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts =
                text.flatMap(
                                (FlatMapFunction<String, WordWithCount>)
                                        (value, out) -> {
                                            for (String word : value.split("\\s")) {
                                                if("gg".equalsIgnoreCase(word)){
                                                    log.error("触发测试错误:{}",word);
                                                    throw new IllegalArgumentException("触发测试错误");
                                                }
                                                out.collect(new WordWithCount(word, 1L));
                                            }
                                        },
                                Types.POJO(WordWithCount.class))
                        .keyBy(value -> value.word)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .reduce((a, b) -> new WordWithCount(a.word, a.count + b.count))
                        .returns(WordWithCount.class);

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // ------------------------------------------------------------------------

    /** Data type for words with count. */
    public static class WordWithCount {

        public String word;
        public long count;

        @SuppressWarnings("unused")
        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}