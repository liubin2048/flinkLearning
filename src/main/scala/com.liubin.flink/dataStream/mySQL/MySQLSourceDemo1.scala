package com.liubin.flink.dataStream.mySQL

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/19
  * Description : JDBC source
  *
  * URL需要注意:查询时会报异常The driver has not received any packets from the server.加上下面的
  * jdbc:mysql://?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false
  */
object MySQLSourceDemo1 {

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://lyytest001:3306/flink?useUnicode=true&characterEncoding=UTF-8"
  val username = "root"
  val password = "root"
  val sqlQuery = "select a1,a2 from test"

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    // 设置flink App并行度
    env.setParallelism(1)
    //添加scala隐式转行
    import org.apache.flink.api.scala._

    val inputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(driver)
      .setDBUrl(url)
      .setUsername(username)
      .setPassword(password)
      .setQuery(sqlQuery)
      // 与setQuery中字段对应
      .setRowTypeInfo(new RowTypeInfo(Types.STRING, Types.INT))
      .finish()

    val mysqlSource = env.createInput(inputFormat)

    mysqlSource.print()

    env.execute("MySQLSourceDemo1")

  }

}
