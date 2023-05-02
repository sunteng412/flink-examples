package com.qingxuan.flink.example.custom.sink;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import java.util.Date;

/**
 * 日志sink
 * @author     : MrFox
 * @date       : 2023/5/2 01:43
 * @description:
 * @version    :
 */
@Slf4j
public class LogSinkFunction<IN> extends RichSinkFunction<IN> {

    /**
     * Writes the given value to the sink. This function is called for every record.
     *
     * <p>You have to override this method when implementing a {@code SinkFunction}, this is a
     * {@code default} method for backward compatibility with the old-style method only.
     *
     * @param value   The input record.
     * @param context Additional context about the input record.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *                   operation to fail and may trigger recovery.
     */
    @Override
    public void invoke(IN value, Context context) throws Exception {
        log.info("qingxuan 当前:{},输出:{}", DateUtil.formatDateTime(new Date()), JSON.toJSONString(value));
    }
}
