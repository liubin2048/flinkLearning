package com.liubin.flink.dataStream.rabbitMQ

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig

/**
  * author : liubin
  * date : 2019/3/20
  * Description : 
  */
object RabbitMQSinkDemo {

  val socketHostname = "lyytest001"
  val socketPort = 9999

  val queueName = "liubin2"

  val host = "lyytest001"
  val virtualHost = "/"
  val port = 5672
  val username = "admin"
  val password = "admin"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream(socketHostname, socketPort)

    val connectionConfig = new RMQConnectionConfig.Builder()
      .setHost(host)
      .setVirtualHost(virtualHost)
      .setPort(port)
      .setUserName(username).setPassword(password)
      .build()

    //注意，换一个新的 queue，否则也会报错
    stream.addSink(new RMQSink(connectionConfig, queueName, new SimpleStringSchema()))
    env.execute("RabbitMQSinkDemo")
  }

}
