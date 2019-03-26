package com.liubin.flink.dataSet.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/13
  * Description : 将数据集1和数据集2进行笛卡尔积操作，创建新数据集。
  */
object CrossDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data1 = List("MI", "HUAWEI Mate")

    val data2 = List(8, 9)

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)

    text1.cross(text2).print()

    env.execute("CrossDemo")

  }
}
