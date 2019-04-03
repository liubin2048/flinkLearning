package com.liubin.flink.dataSet.batchAPI

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/4/3
  * Description : 
  */
object ReduceDemo {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = env.fromElements(1, 2, 3, 4, 5, 6, 7)

    // 对DataSet的元素进行合并，这里是计算累加和
    val test1 = data.reduce(_+_)
    test1.print()

    // 对DataSet的元素进行合并，这里是计算累乘积
    val test2 = data.reduce(_*_)
    test2.print()

    // 对DataSet的元素进行合并，逻辑可以写的很复杂
    val test3 = data.reduce(new ReduceFunction[Int] {
      override def reduce(intermediateResult: Int, next: Int): Int = {
        if (intermediateResult % 2 == 0) {
          intermediateResult + next
        } else {
          intermediateResult * next
        }
      }
    })
    test3.print()

    // 对DataSet的元素进行合并，可以看出intermediateResult是临时合并结果，next是下一个元素
    val test4 = data.reduce(new ReduceFunction[Int] {
      override def reduce(intermediateResult: Int, next: Int): Int = {
        println("intermediateResult=" + intermediateResult + " ,next=" + next)
        intermediateResult + next
      }
    })
    test4.collect()

  }

  case class Wc(line: String, lenght: Int)

}
