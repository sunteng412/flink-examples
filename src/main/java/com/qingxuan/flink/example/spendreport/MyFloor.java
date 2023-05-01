package com.qingxuan.flink.example.spendreport;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 注册一个自己的函数
 * @author     : longchuan
 * @date       : 2023/3/13 10:01 上午
 * @description:
 * @version    :
 */
public class MyFloor extends ScalarFunction {

    public @DataTypeHint("TIMESTAMP(3)") LocalDateTime eval(
        @DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {

        return timestamp.truncatedTo(ChronoUnit.HOURS);
    }
}