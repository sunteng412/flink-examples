package com.qingxuan.flink.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 青玄
 */
@Slf4j
public class MysqlSink extends RichSinkFunction<String> {
    private Connection connection = null;
    Statement sqlExecute;



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            if (connection == null) {
                Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
                connection = DriverManager.getConnection("jdbc:mysql://10.211.55.14:32306/flink_copy?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf8&useSSL=true"
                        , "root", "root");//获取连接
                sqlExecute = connection.createStatement();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static Pattern pattern = Pattern.compile("\\b\\w+\\b");


    @Override
    public void invoke(String value, Context context) {
        SqlSourceRecord parsed = JSON.parseObject(value,SqlSourceRecord.class);
        try {
            if(StringUtils.isBlank(parsed.sql())){
                log.info("sql未解析出来,忽略该sql,{}",value);
                return;
            }

            Matcher matcher = pattern.matcher(parsed.sql());
            if (!matcher.find()) {
                log.error("sql语法解析错误,忽略该sql,{}",value);
            }
            //忽略注释
            String firstToken = matcher.group(0);
            //检查sql
            if(firstToken.equalsIgnoreCase("drop")
            || firstToken.equalsIgnoreCase("create")){
                log.info("检测到危险sql,忽略执行该sql,{}",parsed.sql());
                return;
            }

            log.info("执行sql:{}", parsed.sql());

            sqlExecute.execute(parsed.sql());
        }catch (Exception e){
            log.error("执行错误:{},sql:{}", Throwables.getStackTraceAsString(e),value);
        }

    }

    @Override
    public void close() throws Exception {
        super.close();

        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
