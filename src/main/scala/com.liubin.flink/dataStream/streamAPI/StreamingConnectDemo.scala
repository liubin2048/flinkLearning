package com.liubin.flink.dataStream.streamAPI

import com.bonc.bdev.dataStream.custormSource.{ParallelSource, SingleParallelSource}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/12
  * Description : 通过相互连接不同类型的DataStream输出，创建一个新的ConnectedStreams。
  */
object StreamingConnectDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val text1 = env.addSource(new ParallelSource)
    val text2 = env.addSource(new SingleParallelSource)

    val text2_str = text2.map("str" + _)

    val connectedStreams = text1.connect(text2_str)

    val result = connectedStreams.map(line1=>line1,line2=>line2)

    result.print().setParallelism(1)

    env.execute("StreamingConnectDemo")

  }

}
