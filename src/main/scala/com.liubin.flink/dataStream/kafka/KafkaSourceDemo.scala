package com.liubin.flink.dataStream.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * author : liubin
  * date : 2019/3/11
  * Description : kafka数据源使用样例
  */

object KafkaSourceDemo {

  def main(args: Array[String]): Unit = {

    val topic = "flinkSource"
    val bootstrapServers = "lyytest001:9092,lyytest002:9092,lyytest003:9092"

    // kafka props
    val props = new Properties()
    props.setProperty("bootstrap.servers", bootstrapServers)
    props.setProperty("group.id", "flinkKafka011")
    // 设置kafka消费起始位置,可在该位置，也可在45行进行设置
    // props.setProperty("auto.offset.reset","latest")

    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置flink App并行度
    env.setParallelism(1)
    // 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(5000)
    // 设置流处理时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //添加scala隐式转行
    import org.apache.flink.api.scala._

    val myConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), props)
    // 设置kafka消费起始位置
    myConsumer.setStartFromLatest()
//     myConsumer.setStartFromEarliest()
//     myConsumer.setStartFromTimestamp(...)
//     myConsumer.setStartFromGroupOffsets()（默认）

    val stream = env.addSource(myConsumer)

    stream
      .map(line => {
        val fields = line.split(",")
        msg(fields(0),fields(1).toInt)
      })
      .keyBy("id")
      .timeWindow(Time.seconds(10))
      .sum("money")
      .writeAsCsv("D:/output.txt",FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    env.execute("StreamingKafkaSource")

  }

  case class msg(id: String, money: Int)

}
