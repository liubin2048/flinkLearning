package com.liubin.flink.dataStream.rabbitMQ

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig

/**
  * author : liubin
  * date : 2019/3/20
  * Description : 从 rabbitmq 读取数据
  */
object RabbitMQSourceDemo {

  val queueName = "liubin"

  val host = "lyytest001"
  val virtualHost = "/"
  val port = 5672
  val username = "admin"
  val password = "admin"

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

    // rabbitMQ config
    val config = new RMQConnectionConfig.Builder()
      .setHost(host)
      .setVirtualHost(virtualHost)
      .setPort(port)
      .setUserName(username).setPassword(password)
      .build()

    val stream = env.addSource(new RMQSource(config, queueName, new SimpleStringSchema()))

    stream.setParallelism(1).print()

    env.execute("RabbitMQSourceDemo")

  }

}
