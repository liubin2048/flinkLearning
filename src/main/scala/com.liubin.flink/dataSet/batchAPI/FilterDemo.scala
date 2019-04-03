package com.liubin.flink.dataSet.batchAPI

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/26
  * Description : 自定义过滤器
  */
object FilterDemo {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = env.fromElements(2,4,7,6,8,9)

    val test1 = data.filter(_>5)
    test1.print()

    val test2 = data.filter(new FilterFunction[Int] {
      override def filter(value: Int): Boolean = {
        value % 2 == 0
      }
    })
    test2.print()
  }

}
