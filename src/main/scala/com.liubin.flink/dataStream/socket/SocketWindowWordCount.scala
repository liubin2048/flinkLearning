package com.liubin.flink.dataStream.socket

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author : liubin
  * date : 2019/3/8
  * Description : 滑动窗口计算,每隔1秒统计最近5秒内的数据，打印到控制台
  */

object SocketWindowWordCount {

  def main(args: Array[String]): Unit = {

    val hostname = "lyytest001"

    //获取socket端口号
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e: Exception => {
        System.err.println("No port set. use default port 9999")
      }
        9999
    }

    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //链接socket获取输入数据
    val socketStream = env.socketTextStream(hostname, port)
    //注意：必须要添加这一行隐式转行，否则下面的flatmap方法执行会报错
    //https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/types_serialization.html#type-information-in-the-scala-api
    import org.apache.flink.api.scala._

    //解析数据(把数据打平)，分组，窗口计算，并且聚合求sum
    val wordCount = socketStream
      .flatMap(_.split("\\s"))
      .map(word => (word, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5),Time.seconds(1))  //指定窗口大小，指定间隔时间
      .sum(1)   // sum或者reduce都可以
    //.reduce((a,b)=>(a._1,a._2+b._2))

    //打印到控制台，设置并行度为1(需在print后)
    wordCount.print().setParallelism(1)

    //执行任务
    env.execute("SocketWindowWordCount")

  }
}
