package com.liubin.flink.common.time

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * author : liubin
  * date : 2019/3/11
  * Description : 设置流处理时间特征（事件时间、处理时间、摄取时间）
  */
object EventTimeDemo {

  def main(args: Array[String]): Unit = {

    val topic = "FlinkTest"
    val bootstrapServers = "hadoop001:9092,hadoop002:9092,hadoop003:9092"
    val outputPath = "D:/output.txt"

    val props = new Properties()
    props.setProperty("bootstrap.servers", bootstrapServers)
    props.setProperty("group.id", "flinkKafka011")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //添加scala隐式转行
    import org.apache.flink.api.scala._

    val stream = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), props))

    stream
      .map(_.split(",")).keyBy(1) //以逗号切分后按第二个字段分组
      .timeWindow(Time.hours(1)) //按小时为窗口进行统计
      .sum(1) //假设第二个字段为金额
      .writeAsCsv(outputPath).setParallelism(1)

    env.execute("EventTimeDemo")

  }

}
