package com.qingxuan.flink.cdc;

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ververica.cdc.debezium.DebeziumDeserializationSchema
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.util.Collector
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct
import com.qingxuan.flink.cdc.SqlSourceRecord
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema.STRING_SCHEMA
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaBuilder.struct
import lombok.extern.slf4j.Slf4j
import org.apache.commons.lang3.StringUtils

import java.util.concurrent.atomic.AtomicInteger
import scala.util.matching.Regex


/**
 * 自定义sql conecter source
 * @Author: 龙川
 * @Date: 2021/12/29  
 * @Desc:
 * */
@Slf4j
class CustomerMysqlDebeziumDeserialization extends DebeziumDeserializationSchema[String] {


  def getInsertV(struct: Struct): String = {
    val valueStruct = struct.getStruct("after")
    val fields = valueStruct.schema().fields()

    val valueStr = new StringBuilder
    val idx = new AtomicInteger()

    val iter = fields.iterator()
    while (iter.hasNext) {
      if (idx.get() != 0) {
        valueStr.append(",")
      }
      idx.incrementAndGet()

      val field = iter.next()
      val value = valueStruct.get(field)

      if (value.isInstanceOf[Number]) {
        valueStr.append(value)
      } else {
        valueStr.append("\"" + value + "\"")
      }
    }

    "insert into " +
      struct.getStruct("source")
        .getString("table") +
      " values(" + valueStr.toString() + ");"
  }


  def getDeleteV(struct: Struct): String = {
    val valueStructBefore = struct.getStruct("before")
    "delete from " + struct.getStruct("source").getString("table") + " where " + " id = " +
      valueStructBefore.get("id")

  }

  def getUpdateV(struct: Struct): String = {
    val valueStruct = struct.getStruct("after")
    val valueStructBefore = struct.getStruct("before")

    val fields = valueStruct.schema().fields()

    val valueStr = new StringBuilder
    val idx = new AtomicInteger()

    val iter = fields.iterator()
    while (iter.hasNext) {
      if (idx.get() != 0) {
        valueStr.append(",")
      }

      val field = iter.next()
      val value = valueStruct.get(field)

      if (!value.equals(valueStructBefore.get(field))) {
        valueStr.append(field.name()).append("=")
        if (value.isInstanceOf[Number]) {
          valueStr.append(value)
        } else {
          valueStr.append("\"" + value + "\"")
        }

        idx.incrementAndGet()
      }
    }

    "update " +
      struct.getStruct("source").getString("table") +
      " set " + valueStr.toString() + " where " + "id = " + valueStruct.get("id") + ";"

  }

  /**
   * ddl {"source":{"file":"mysql-bin.000003","pos":219,"server_id":1},"position":{"transaction_id":null,"ts_sec":1689499867,"file":"mysql-bin.000003","pos":361,"server_id":1},"databaseName":"flink","ddl":"create database flink","tableChanges":[]}
   * */
  def getDDL(source: String): String = {
    val jsonObj = JSON.parseObject(source)

    //判断是否是ddl
    if (jsonObj.containsKey("ddl")) {
      var ddlSql = jsonObj.getString("ddl")

      //替换掉注释
      val pattern: Regex = """\/\*.*\*\/""".r
      ddlSql = pattern.replaceAllIn(ddlSql, "")

      //判断是否包含库名
      if (ddlSql.contains(".")) {
        //去除掉库名
        val parts = jsonObj.getString("ddl").trim.split("\\s+")

        val tmpDDL = new StringBuilder()
        for (a <- parts) {
          if (a.contains(".")) {
            tmpDDL.append(a.split("\\.")(1)).append(" ")
          } else {
            tmpDDL.append(a).append(" ")
          }
        }

        return tmpDDL.toString()
      } else {
        return ddlSql
      }

    }
    println("找不到ddl语句,跳过:" + source)
    ""
  }

  /**
   * 解析sql
   * */
  def resolveSql(struct: Struct): String = {
    var source = ""
    var op = ""

    //ddl会包含historyRecord
    try {
      source = struct.getString("historyRecord")
      op = "ddl"
    } catch {
      case _: Exception => {
        op = struct.getString("op")
      }
    }

    val jsonBean = new JSONObject()


    jsonBean.put("sourceValue", source)
    jsonBean.put("sql", op match {
      case "c" => getInsertV(struct)
      case "d" => getDeleteV(struct)
      case "u" => getUpdateV(struct)
      case "ddl" => getDDL(source)
      case _ => ""
    })

    jsonBean.toJSONString
  }


  /**
   * 封装的数据：
   * {
   * "database":"",
   * "tableName":"",
   * "type":"c r u d",
   * "before":"",
   * "after":"",
   * "ts": ""
   *
   * }
   *
   * @param sourceRecord
   * @param collector
   */
  override
  def deserialize(sourceRecord: SourceRecord, collector: Collector[String]): Unit = {
    val sql = resolveSql(sourceRecord.value().asInstanceOf[Struct])


    collector.collect(sql)

  }


  override def getProducedType: TypeInformation[String] = {
    BasicTypeInfo.STRING_TYPE_INFO
  }


}

