package com.liubin.flink.dataStream.custormSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author : liubin
  * date : 2019/3/12
  * Description : 测试RichParallelSource自定义数据源，并指定多并行度，每5秒统计一次总和
  */
object RichParallelSourceStreamingDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    // 指定并行度为2
    val stream = env.addSource(new RichParallelSource).setParallelism(2)

    val mapData = stream.map(line=>{
      println("接收到的数据："+line)
      line
    })

    val sum = mapData.timeWindowAll(Time.seconds(5)).sum(0)

    sum.print().setParallelism(1)

    env.execute("RichParallelSourceStreamingDemo")



  }

}
