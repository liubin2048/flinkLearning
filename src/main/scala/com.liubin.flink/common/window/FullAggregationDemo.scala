package com.liubin.flink.common.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * author : liubin
  * date : 2019/3/13
  * Description : Window全量聚合：等属于窗口的数据到齐，才开始进行聚合计算【可以实现对窗口内的数据进行排序等需求】
  * - apply(windowFunction)
  * - process(processWindowFunction)
  * - processWindowFunction比windowFunction提供了更多的上下文信息。
  */
object FullAggregationDemo {

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
      .process(new ProcessWindowFunction[(Int, String), String, Tuple, TimeWindow] {
        override def process(key: Tuple, context: Context, elements: Iterable[(Int, String)], out: Collector[String]): Unit = {
          var count = 0
          for (element <- elements) {
            count += 1
          }
          out.collect("window:" + context.window + ",count:" + count)
        }
      }).print()

    env.execute("FullAggregationDemo")


  }
}
