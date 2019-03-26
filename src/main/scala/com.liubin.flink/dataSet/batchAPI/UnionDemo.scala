package com.liubin.flink.dataSet.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * author : liubin
  * date : 2019/3/13
  * Description : 并集 union
  */
object UnionDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data1 = ListBuffer[Tuple2[Int,String]]((1,"la"),(2,"lb"),(3,"lc"))
    val data2 = ListBuffer[Tuple2[Int,String]]((6,"shanghai"),(2,"beijing"),(4,"guangzhou"))

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)

    text1.union(text2).print()

    env.execute("UnionDemo")

  }

}
