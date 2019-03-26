package com.liubin.flink.dataStream.mySQL

import java.sql.Types

import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.types.Row

/**
  * author : liubin
  * date : 2019/3/19
  * Description : JDBC sink 插入数据到mysql中
  */
object MySQLSinkDemo1 {

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://lyytest001:3306/flink?useUnicode=true&characterEncoding=UTF-8"
  val username = "root"
  val password = "root"
  val sqlQuery = "insert into test values(?,?,?)"


  val row1 = new Row(2)
  row1.setField(0, 1001)
  row1.setField(1, "describe")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // 设置此可以屏蔽掉日记打印情况
    env.getConfig.disableSysoutLogging()
    // 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(5000)
    // 设置流处理时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val stream = env.socketTextStream("lyytest001", 9999)
      .flatMap(_.split("\\s"))
      .keyBy(0)
      .sum(1)

    val jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername(driver)
      .setDBUrl(url)
      .setUsername(username)
      .setPassword(password)
      .setQuery(sqlQuery)
      // 与setQuery中字段对应
      .setSqlTypes(Array[Int](Types.INTEGER, Types.VARCHAR))
      .finish()

    // 连接到目标数据库，并初始化preparedStatement
    jdbcOutputFormat.open(0, 1)

    // 添加记录到 preparedStatement,此时jdbcOutputFormat需要确保是开启的(未指定列类型时，此操作可能会失败)
    jdbcOutputFormat.writeRecord(row1)
    // 执行preparedStatement，并关闭此实例的所有资源
    jdbcOutputFormat.close()

    env.execute("MySQLSinkDemo1")

  }

}
