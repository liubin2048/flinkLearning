package com.liubin.flink.dataSet.batchAPI

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/4/3
  * Description : 
  */
object MapDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = env.fromElements("flink vs spark", "buffer vs  shuffer")

    // 以element为粒度，将element进行map操作，转化为大写并添加后缀字符串"--##bigdata##"
    val test2 = data.map(_.toUpperCase() + "--##bigdata##")
    test2.print()

    // 以element为粒度，将element进行map操作，转化为大写并,并计算line的长度。
    val test3 = data.map(line => (line.toUpperCase, line.length))
    test3.print()

    // 以element为粒度，将element进行map操作，转化为大写并,并计算line的长度。
    val test4 = data.map(line => (Wc(line.toUpperCase(), line.length)))
    test4.print()

  }

  case class Wc(line: String, lenght: Int)

}


