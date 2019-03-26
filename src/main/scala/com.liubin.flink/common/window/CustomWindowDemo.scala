package com.liubin.flink.common.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, EventTimeTrigger, ProcessingTimeTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

/**
  * author : liubin
  * date : 2019/3/12
  * Description :
  * 自定义的Window需要指定3个function
  *   1.Window Assigner：负责将元素分配到不同的window
  *   2.Trigger：触发器，定义何时或什么情况下触发一个window
  *   3.Evictor：驱逐者，即保留上一window留下的某些元素
  *
  * 最简单的情况，如果业务不是特别复杂，仅仅是基于Time和Count
  * 则可以用系统定义好的WindowAssigner以及Trigger和Evictor来实现不同的组合：
  * 例如：基于Event Time，每5秒内的数据为界，以每秒的滑动窗口速度进行operator操作
  * 但是当且仅当5秒内的元素数达到100时才触发窗口，触发时保留上个窗口的10个元素
  *
  * keyedStream
  * .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1))
  * .trigger(CountTrigger.of(100))
  * .evictor(CountEvictor.of(10));
  */

object CustomWindowDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val source = env.socketTextStream("lyytest001", 9000)

    val values = source.flatMap(value => value.split("\\s+")).map(value => (value, 1))

    val keyValue = values.keyBy(0)

    // 定义计数窗口(一旦窗口中的元素数量超过给定限制就会触发，窗口不清除)
    val countWindowWithoutPurge = keyValue.window(GlobalWindows.create()).
      trigger(CountTrigger.of(2))

    // 定义计数窗口( 将其作为另一个触发器的参数，并将其转换为带有清除功能(transforms it into a purging one)的窗口)
    val countWindowWithPurge = keyValue.window(GlobalWindows.create()).
      trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](2)))

    countWindowWithoutPurge.sum(1).print()

    countWindowWithPurge.sum(1).print()

    env.execute("CustomWindowDemo")

  }

}
