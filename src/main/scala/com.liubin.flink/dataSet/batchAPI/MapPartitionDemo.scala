package com.liubin.flink.dataSet.batchAPI

import java.lang

import org.apache.flink.api.common.functions.MapPartitionFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * author : liubin
  * date : 2019/4/3
  * Description : 
  */
object MapPartitionDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = env.fromElements("flink vs spark", "buffer vs  shuffer")

    // 以partition为粒度，进行map操作，计算element个数
    val test1 = data.mapPartition(new MapPartitionFunction[String, String]() {
      override def mapPartition(values: lang.Iterable[String], out: Collector[String]): Unit = {
        val itor = values.iterator()
        while (itor.hasNext) {
          val line = itor.next().toUpperCase + "--##bigdata##"
          out.collect(line)
        }
      }
    })
    test1.print()

    // 以partition为粒度，进行map操作，转化为大写并,并计算line的长度。
    val test4 = data.mapPartition(new MapPartitionFunction[String, Wc] {
      override def mapPartition(values: lang.Iterable[String], out: Collector[Wc]): Unit = {
        val itor = values.iterator
        while (itor.hasNext) {
          var s = itor.next()
          out.collect(Wc(s.toUpperCase(), s.length))
        }
      }
    })
    test4.print()
  }

  case class Wc(line: String, lenght: Int)

}
