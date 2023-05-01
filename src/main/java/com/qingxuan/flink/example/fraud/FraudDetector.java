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
 * 第一版：对于一个账户，如果出现小于 $1 美元的交易后紧跟着一个大于 $500 的交易，就输出一个报警信息
 * @author     : longchuan
 * @date       : 2023/3/2 7:15 下午
 * @description:
 * @version    :
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    /**
     * 状态
     * */
    private transient ValueState<Boolean> flagState;

    /**
     * 注册状态
     * */
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> stateDescriptor =
                new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(stateDescriptor);
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
        }


//        Alert alert = new Alert();
//        alert.setId(transaction.getAccountId());
//
//        collector.collect(alert);
    }
}