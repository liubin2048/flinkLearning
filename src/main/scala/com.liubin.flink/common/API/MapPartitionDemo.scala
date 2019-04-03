package com.liubin.flink.common.API

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * author : liubin
  * date : 2019/3/13
  * Description : MapPartititon 通过将给定的函数应用于数据集的每个并行分区
  */
object MapPartitionDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = List[String]("hello word", "apache flink")
    val text = env.fromCollection(data)

    text.mapPartition(lines => {
      //创建数据库连接，建议将这块代码放到try-catch代码块中
      //此时是一个分区的数据获取一次连接【优点，每个分区获取一次链接】,values中保存了一个分区的数据

      val res = ListBuffer[String]()

      while (lines.hasNext) {
        val line = lines.next()
        val words = line.split("\\W+")
        res.append(words(1))
      }
      res
      //关闭连接
    }).print()

  }
}
