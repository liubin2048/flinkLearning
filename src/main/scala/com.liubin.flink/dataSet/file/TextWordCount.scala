package com.liubin.flink.dataSet.file

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/8
  * Description : 文本词频统计，输出出现次数大于5的单词
  */

object TextWordCount {

  def main(args: Array[String]): Unit = {

    val inputPath = "D:/input.txt"
    val outputPath = "D:/output.txt"

    //获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //链接file获取输入数据
    val text = env.readTextFile(inputPath)
    //注意：必须要添加这一行隐式转行，否则下面的flatmap方法执行会报错
    //https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/types_serialization.html#type-information-in-the-scala-api
    import org.apache.flink.api.scala._

    val wordCount = text.flatMap(_.split("\\s")).map(word => (word,1))
      .groupBy(0)
      .sum(1)
      .filter(_._2 > 5)

    //当不设置setParallelism时，会生成多个文件（并行度决定），指定为1时写入一个文件
    wordCount.writeAsCsv(outputPath).setParallelism(1)

    env.execute("TextWordCount")

  }

}
