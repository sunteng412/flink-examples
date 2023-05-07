package com.qingxuan.flink.example.custom.count_wind_trigger;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.shaded.com.jayway.jsonpath.internal.function.numeric.Sum;

import java.util.Objects;

/**********************
 * 自定义带超时 滑动窗口触发
 * @Description TODO
 * @Date 2023/5/6 5:58 PM
 * @Created by 龙川
 ***********************/
@Slf4j
public class CustomCountWindTrigger<T> extends Trigger<T, TimeWindow> {

    /**
     * 窗口最大数据量
     */
    private int maxCount;
    /**
     * event time / process time
     */
    private TimeCharacteristic timeType;

    /**
     * 计数方法
     */
    class Sum implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }

    public CustomCountWindTrigger(int maxCount, TimeCharacteristic timeType) {

        this.maxCount = maxCount;
        this.timeType = timeType;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        ReducingState<Long> partitionedState = ctx.getPartitionedState(countStateDescriptor);
        if(Objects.nonNull(partitionedState.get())){
            log.info("开始merge:{},{},{}",window.getStart(),window.getEnd(),partitionedState.get());
            partitionedState.add(partitionedState.get());
        }
    }

    @Override
    public boolean canMerge() {
        return Boolean.TRUE;
    }

    private TriggerResult fireAndPurge(TimeWindow window, TriggerContext ctx) throws Exception {
        clear(window, ctx);
        return TriggerResult.FIRE_AND_PURGE;
    }

    /**
     * 用于储存窗口当前数据量的状态对象
     */
    private ReducingStateDescriptor<Long> countStateDescriptor =
            new ReducingStateDescriptor("counter",new Sum(), LongSerializer.INSTANCE);


    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
        countState.add(1L);

        if (countState.get() >= maxCount) {
            log.info("数量到达:{},{}" ,element,countState.get());
            return fireAndPurge(window, ctx);
        }
        if (timestamp >= window.getEnd()) {
            log.info("时间到达:{},{}" ,element,countState.get());
            return fireAndPurge(window, ctx);
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (timeType != TimeCharacteristic.ProcessingTime) {
            return TriggerResult.CONTINUE;
        }

        ReducingState<Long> partitionedState = ctx.getPartitionedState(countStateDescriptor);
        if (time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        } else {
            log.info("触发处理时间: {},{}" , time,partitionedState.get());
            return fireAndPurge(window, ctx);
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (timeType != TimeCharacteristic.EventTime) {
            return TriggerResult.CONTINUE;
        }

        if (time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        } else {
            log.info("fire with event time: " + time);
            return fireAndPurge(window, ctx);
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
        countState.clear();
    }
}
