package com.liubin.flink.common.window

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


  }
}
