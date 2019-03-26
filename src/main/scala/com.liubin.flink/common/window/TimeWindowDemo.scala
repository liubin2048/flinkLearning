package com.liubin.flink.common.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author : liubin
  * date : 2019/3/12
  * Description : 时间窗口TimeWindow（TumblingWindow滚动窗口，SlidingWindow滑动窗口）
  */
object TimeWindowDemo {

  def main(args: Array[String]): Unit = {

    val hostname = "lyytest001"
    val port = 9999

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val socketStream = env.socketTextStream(hostname, port)

    // Stream of (sensorId, carCnt)
    val vehicleCnts: DataStream[(Int, Int)] = socketStream.map(line => {
      val fields = line.split(",")
      (fields(0).toInt, fields(1).toInt)
    })

    // TumblingWindow滚动窗口
    val tumblingCnts: DataStream[(Int, Int)] = vehicleCnts
      // key stream by sensorId
      .keyBy(0)
      // 滚动时间窗口的长度为1分钟
      .timeWindow(Time.minutes(1))
      // compute sum over carCnt
      .sum(1)

    // SlidingWindow滑动窗口
    val slidingCnts: DataStream[(Int, Int)] = vehicleCnts
      .keyBy(0)
      // 滑动时间窗口长度为1分钟，滑动触发间隔30秒
      .timeWindow(Time.minutes(1), Time.seconds(30))
      .sum(1)

    tumblingCnts.print().setParallelism(1)
    slidingCnts.print().setParallelism(1)

    env.execute("TimeWindowDemo")

  }

}
