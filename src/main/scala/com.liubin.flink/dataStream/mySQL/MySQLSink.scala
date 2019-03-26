package com.liubin.flink.dataStream.mySQL

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.slf4j.LoggerFactory

/**
  * author : liubin
  * date : 2019/3/20
  * Description : 数据批量 sink 数据到 mysql
  */
class MySQLSink extends RichSinkFunction[List[Account]] {

  val logger = LoggerFactory.getLogger(classOf.getName)

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://lyytest001:3306/flink?useUnicode=true&characterEncoding=UTF-8"
  val username = "root"
  val password = "root"
  val sql = "insert into test values(?,?,?,?)"

  var preparedStatement: PreparedStatement = null
  var connection: Connection = null

  // open方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 加载JDBC驱动
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    preparedStatement = connection.prepareStatement(sql)

  }

  // 每条数据的插入都要调用一次 invoke() 方法
  override def invoke(value: List[Account]): Unit = {
    for (account: Account <- value) {
      preparedStatement.setInt(1, account.id)
      preparedStatement.setString(2, account.username)
      preparedStatement.setString(3, account.password)
      preparedStatement.setInt(4, account.age)
      preparedStatement.setTimestamp(5, new Timestamp(System.currentTimeMillis()))
    }
    val count = preparedStatement.executeBatch //批量后执行
    logger.info("成功了插入了" + count.length + "行数据")

  }

  override def close(): Unit = {
    super.close()
    if (preparedStatement != null) {
      preparedStatement.close()
    }
    if (connection != null) {
      connection.close()
    }
  }


}

