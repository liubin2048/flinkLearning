package com.liubin.flink.dataSet.batchAPI

import java.lang

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * author : liubin
  * date : 2019/4/3
  * Description :
  */
object ReduceGroupDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = env.fromElements(1, 2, 3, 4, 5, 6, 7)

    //对DataSet的元素进行分组合并，这里是计算累加和
    val test1 = data.reduceGroup(new GroupReduceFunction[Int,Int] {
      override def reduce(values: lang.Iterable[Int], out: Collector[Int]): Unit = {
        var sum = 0
        val itor = values.iterator()
        while(itor.hasNext){
          sum += itor.next()
        }
        out.collect(sum)
      }
    })

    test1.print()

    //对DataSet的元素进行分组合并，这里是分别计算偶数和奇数的累加和
    val test2 = data.reduceGroup(new GroupReduceFunction[Int,(Int,Int)] {
      override def reduce(values: lang.Iterable[Int], out: Collector[(Int, Int)]): Unit = {
        var sum1 = 0
        var sum2 = 0
        val itor = values.iterator()
        while(itor.hasNext){
          val v = itor.next()
          if(v%2 == 0){
            sum1 += v
          }else {
            sum2 += v
          }
        }
        out.collect(sum1,sum2)
      }
    })

    test2.print()

    // 对DataSet的元素进行分组合并，这里是对分组后的数据进行合并操作，统计每个人的工资总和（每个分组会合并出一个结果）
    val data2 = env.fromElements(("zhangsan", 1000), ("lisi", 1001), ("zhangsan", 3000), ("wangwu", 1002))
    val test3 = data2.groupBy(0).reduceGroup(new GroupReduceFunction[(String,Int),(String,Int)] {
      override def reduce(values: lang.Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {

        var salary = 0
        var name = ""
        val itor = values.iterator()
        while(itor.hasNext){
          val t = itor.next()
          name = t._1
          salary += t._2

        }
        out.collect(name,salary)
      }
    })

    test3.print()
  }
}
