package com.liubin.flink.dataSet.collection

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/7
  * Description : 集合词频统计
  */

object CollectionWordCount {

  def main(args: Array[String]): Unit = {

    val collection = Array("a","b","c","a","c")

    //获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //注意：必须要添加这一行隐式转行
    //https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/types_serialization.html#type-information-in-the-scala-api
    import org.apache.flink.api.scala._

    val data = env.fromCollection(collection)
    data.map(word =>(word,1)).groupBy(0).sum(1).print()

  }

}
