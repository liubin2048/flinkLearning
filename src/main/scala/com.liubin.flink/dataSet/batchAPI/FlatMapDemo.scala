package com.liubin.flink.dataSet.batchAPI

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * author : liubin
  * date : 2019/3/26
  * Description :
  */
object FlatMapDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = env.fromElements("flink vs spark", "buffer vs  shuffer")

    // 以element为粒度，将element进行map操作，转化为大写并添加后缀字符串"--##bigdata##"
    val test1 = data.flatMap(new FlatMapFunction[String, String] {
      override def flatMap(value: String, out: Collector[String]): Unit = {
        out.collect(value.toUpperCase + "--##bigdata##")
      }
    })
    test1.print()

    // 对每句话进行单词切分,一个line可以转化为多个Word
    val test2 = data.flatMap(new FlatMapFunction[String,Array[String]] {
      override def flatMap(value: String, out: Collector[Array[String]]): Unit = {
        val arr = value.toUpperCase().split("\\s+")
        out.collect(arr)
      }
    })
    // 先获取Array[String],再从中获取到String
    test2.collect().foreach(_.foreach(println(_)))

  }

}
