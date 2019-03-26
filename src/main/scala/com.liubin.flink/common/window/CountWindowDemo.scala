package com.liubin.flink.common.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * author : liubin
  * date : 2019/3/12
  * Description : 统计窗口TimeWindow（TumblingWindow滚动窗口，SlidingWindow滑动窗口）
  */
object CountWindowDemo {

  def main(args: Array[String]): Unit = {

    val hostname = "lyytest001"
    val port = 9999

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val socketStream = env.socketTextStream(hostname, port)

    // Stream of(sensorId, carCnt)
    val vehicleCnts: DataStream[(Int, Int)] = socketStream.map(line => {
      val fields = line.split(",")
      (fields(0).toInt, fields(1).toInt)
    })

    // TumblingWindow滚动窗口
    val tumblingCnts: DataStream[(Int, Int)] = vehicleCnts
      // key stream by sensorId
      .keyBy(0)
      // 滚动窗口统计100个元素的大小
      .countWindow(100)
      // compute sum over carCnt
      .sum(1)

    // SlidingWindow滑动窗口
    val slidingCnts: DataStream[(Int, Int)] = vehicleCnts
      .keyBy(0)
      // 滑动窗口统计100个元素大小，每10个元素触发一次
      .countWindow(100, 10)
      .sum(1)

    tumblingCnts.print().setParallelism(1)
    slidingCnts.print().setParallelism(1)

    env.execute("CountWindowDemo")

  }

}
