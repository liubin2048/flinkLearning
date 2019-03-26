package com.liubin.flink.dataSet.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/13
  * Description : dataSet join操作
  */
object JoinDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //tuple2[用户id，用户姓名]
    val data1 = List[Tuple2[Int, String]]((1, "la"), (2, "lb"), (3, "lc"))
    //tuple2[用户id，用户所在城市]
    val data2 = List[Tuple2[Int, String]]((1, "shanghai"), (2, "beijing"), (3, "guangzhou"))

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)

    // 创建一个新的[[DataSet]]，其中每对连接的元素的结果都是结果
    text1.join(text2)
      .where(0) //指定第一个数据集中需要进行比较的元素角标
      .equalTo(0) //指定第二个数据集中需要进行比较的元素角标
      .apply((first, second) => {
      (first._1, first._2, second._2)
    })
      .print()

    env.execute("JoinDemo")

  }


}
