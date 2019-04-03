package com.liubin.flink.dataStream.streamAPI

import com.liubin.flink.dataStream.custormSource.SingleParallelSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/12
  * Description : 通过相互合并相同类型的数据流输出，创建一个新的数据流。
  */
object StreamingUnionDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val text1 = env.addSource(new SingleParallelSource)
    val text2 = env.addSource(new SingleParallelSource)

    val unionStream = text1.union(text2)

    val result = unionStream.map(line => {
      println("接收到的数据：" + line)
      line
    })

    result.print().setParallelism(1)

    env.execute("StreamingUnionDemo")
  }

}
