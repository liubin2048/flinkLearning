package com.liubin.flink.common.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author : liubin
  * date : 2019/3/13
  * Description : Window增量聚合：窗口中每进入一条数据，就进行一次计算
  * - reduce(reduceFunction)
  * - aggregate(aggregateFunction)
  * - sum(),min(),max()
  */
object IncrementAggregationDemo {

  def main(args: Array[String]): Unit = {


    val hostname = "lyytest001"
    val port = 9999

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val stream = env.socketTextStream(hostname, port)

    val data = stream
      .map(line => (1, line))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce((x, y) => (x._1, x._2 + " " + y._2))
      .print()

    env.execute("IncrementAggregationDemo")

  }

}
