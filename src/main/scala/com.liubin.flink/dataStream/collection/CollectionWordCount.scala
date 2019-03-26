package com.liubin.flink.dataStream.collection

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/7
  * Description : 集合词频统计
  */

object CollectionWordCount {

  def main(args: Array[String]): Unit = {

    val collection = Array("a","b","c","a","c")

    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //注意：必须要添加这一行隐式转行
    //https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/types_serialization.html#type-information-in-the-scala-api
    import org.apache.flink.api.scala._

    val data = env.fromCollection(collection)
    data
      .map(word =>(word,1))
      .keyBy(0)
      .sum(1)
      .print()
      .setParallelism(1)      //使用一个单线程打印结果，并行度为1(需在print后)

    env.execute("CollectionWordCount")

  }

}
