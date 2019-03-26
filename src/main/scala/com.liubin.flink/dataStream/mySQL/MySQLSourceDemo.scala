package com.liubin.flink.dataStream.mySQL

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * author : liubin
  * date : 2019/3/20
  * Description :
  */
object MySQLSourceDemo {

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

    val dataStream:DataStream[(String, String)] = env.addSource(new MySQLSource())

    dataStream.print().setParallelism(1)

    env.execute("MySQLSourceDemo")

  }

}
