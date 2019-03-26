package com.liubin.flink.dataStream.mySQL

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory

/**
  * author : liubin
  * date : 2019/3/20
  * Description : 
  */
class MySQLSource extends RichSourceFunction[Tuple2[String, String]] {

  val logger = LoggerFactory.getLogger(classOf.getName)

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://lyytest001:3306/flink?useUnicode=true&characterEncoding=UTF-8"
  val username = "root"
  val password = "root"
  val sql = "select a,b from test"

  var connection: Connection = null
  var preparedStatement: PreparedStatement = null

  // open方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    preparedStatement = connection.prepareStatement(sql)
  }

  // 执行查询并获取结果
  override def run(ctx: SourceFunction.SourceContext[Tuple2[String, String]]): Unit = {
    val resultSet = preparedStatement.executeQuery()
    while (resultSet.next()) {
      val id = resultSet.getString("id")
      val name = resultSet.getString("name")
      val tuple = Tuple2(id, name)
      ctx.collect(tuple)
    }
  }

  override def cancel(): Unit = {
    super.close()
    if (connection != null) {
      connection.close()
    }
    if (preparedStatement != null) {
      preparedStatement.close()
    }

  }
}
