package com.liubin.flink.dataSet.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/13
  * Description : 外连接(左外连接、右外连接、全外连接)
  */
object OutJoinDemo {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data1 = List[Tuple2[Int, String]]((1, "la"), (2, "lb"), (3, "lc"))
    val data2 = List[Tuple2[Int, String]]((1, "shanghai"), (2, "beijing"), (3, "guangzhou"))

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)

    text1.leftOuterJoin(text2).where(0).equalTo(0).apply((first, second) => {
      if (second == null) {
        (first._1, first._2, "null")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()

    println("===============================")

    text1.rightOuterJoin(text2).where(0).equalTo(0).apply((first, second) => {
      if (first == null) {
        (second._1, "null", second._2)
      } else {
        (first._1, first._2, second._2)
      }
    }).print()


    println("===============================")

    text1.fullOuterJoin(text2).where(0).equalTo(0).apply((first, second) => {
      if (first == null) {
        (second._1, "null", second._2)
      } else if (second == null) {
        (first._1, first._2, "null")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()

    env.execute("OutJoinDemo")

  }

}
