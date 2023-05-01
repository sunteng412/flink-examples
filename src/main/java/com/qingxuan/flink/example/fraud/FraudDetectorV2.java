package com.qingxuan.flink.example.fraud;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.util.Objects;

/**
 * 欺诈监测
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/try-flink/datastream/#%E5%9C%A8-ide-%E4%B8%AD%E8%BF%90%E8%A1%8C
 * 第二版：对于一个账户，1分钟内同时出现小于 $1 美元的交易后跟着一个大于 $500 的交易，就输出一个报警信息，
 * 骗子们在小额交易后不会等很久就进行大额消费，这样可以降低小额测试交易被发现的几率。
 * 比如，假设你为欺诈检测器设置了一分钟的超时，对于上边的例子，交易 3 和 交易 4 只有间隔在一分钟之内才被认为是欺诈交易。
 * Flink 中的 KeyedProcessFunction 允许您设置计时器，该计时器在将来的某个时间点执行回调函数。
 * @author     : longchuan
 * @date       : 2023/3/2 7:15 下午
 * @description:
 * @version    :
 */
public class FraudDetectorV2 extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    /**
     * 状态-记录上次的触发
     * */
    private transient ValueState<Boolean> flagState;

    /**
     * 状态-记录上次的触发时间
     * */
    private transient ValueState<Long> timerState;

    /**
     * 注册状态
     * */
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> stateDescriptor =
                new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(stateDescriptor);

        ValueStateDescriptor<Long> timerDescriptor =
                new ValueStateDescriptor<>("timer-state", Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

            //获取当前的状态
        Boolean lastTransactionWasSmall = flagState.value();

        //判断状态 --- 上次是否属于小金额
        if(Objects.nonNull(lastTransactionWasSmall)){
            //判断这次的金额是否大于大金额500阈值
            if(transaction.getAmount() > LARGE_AMOUNT){
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                //收集处理，判断欺诈
                collector.collect(alert);
            }

            //清除状态，即继续判断下次是不是
            flagState.clear();
        }

        //如果本次
        if(transaction.getAmount() < SMALL_AMOUNT){
            //设置标识为true
            flagState.update(Boolean.TRUE);

            //设置时间状态，使用的是事件时间模式
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            //定时器触发后，将会调用onTimer方法
            context.timerService().registerEventTimeTimer(timer);
            timerState.update(timer);
        }

    }

    /**
     * 定时器触发时调用
     * @param
     * @return
     * @description:
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        //去除状态
        timerState.clear();
        flagState.clear();
    }

    /**
     * 清除状态
     * @param
     * @return
     * @description:
     */
    private void cleanUp(Context ctx) throws Exception {
        // 清除定时器
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // 清除状态
        timerState.clear();
        flagState.clear();
    }


}