package com.liubin.flink.dataSet.file

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/26
  * Description : 
  */
object HdfsFileWordCount {

  def main(args: Array[String]): Unit = {
    //1.创建批处理环境
    val  env = ExecutionEnvironment.getExecutionEnvironment
    //注意：必须要添加这一行隐式转行，否则下面的flatmap方法执行会报错
    //https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/types_serialization.html#type-information-in-the-scala-api
    import org.apache.flink.api.scala._

    //2.HDFS数据
    val text = env.readTextFile("hdfs://172.16.13.116:9000/test")

    //3.执行运算
    val counts = text.flatMap(_.toLowerCase.split("\\W+")).map((_, 1)).groupBy(0).sum(1)

    //4.将结果打印出来
    counts.print()

  }

}
