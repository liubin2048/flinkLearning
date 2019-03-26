package com.liubin.flink.dataStream.custormSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author : liubin
  * date : 2019/3/12
  * Description : 测试SingleParallelSource自定义数据源，每5秒统计一次总和
  */
object SingleParallelSourceStreamingDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val stream = env.addSource(new SingleParallelSource)
    // 无法设置多并行度，会抛出异常
//    val stream = env.addSource(new NoParallelSource).setParallelism(2)

    val mapData = stream.map(line=>{
      println("接收到的数据："+line)
      line
    })

    val sum = stream.timeWindowAll(Time.seconds(5)).sum(0)

    sum.print().setParallelism(1)

    env.execute("SingleParallelSourceStreamingDemo")

  }

}
