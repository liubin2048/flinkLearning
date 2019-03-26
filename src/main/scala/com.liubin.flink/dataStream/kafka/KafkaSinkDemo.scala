package com.liubin.flink.dataStream.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
  * author : liubin
  * date : 2019/3/11
  * Description : kafka输出源使用样例
  */

object KafkaSinkDemo {

  def main(args: Array[String]): Unit = {

    val topic = "flinkSink"
    val bootstrapServers = "lyytest001:9092,lyytest002:9092,lyytest003:9092"

    val kafkaPorducer = new FlinkKafkaProducer011[String](bootstrapServers,topic,new SimpleStringSchema())
    // kafka版本0.10+允许在将记录写入Kafka时附加记录的事件时间戳;此方法不适用于早期Kafka版本
    kafkaPorducer.setWriteTimestampToKafka(true)

    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置flink App并行度
    env.setParallelism(1)
    // 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(5000)
    // 设置流处理时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("lyytest001",9999)
    stream.addSink(kafkaPorducer)

    env.execute("KafkaSinkDemo")

  }
}
