package com.liubin.flink.dataStream.streamAPI

import com.bonc.bdev.dataStream.custormSource.SingleParallelSource
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/13
  * Description : 自定义分区器:按奇偶数分区
  */

class CustormPartitioner extends Partitioner[Long] {

  override def partition(key: Long, numPartitions: Int): Int = {

    println("分区总数：" + numPartitions)

    if (key % 2 == 0) {
      0
    } else {
      1
    }

  }

}
object CustormPartitionerDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 隐式转换
    import org.apache.flink.api.scala._

    val text = env.addSource(new SingleParallelSource)

    // 把long类型的数据转成tuple类型
    val tupleData = text.map(line => {
      Tuple1(line) // 注意tuple1的实现方式
    })

    val partitionData = tupleData.partitionCustom(new CustormPartitioner, 0)

    val result = partitionData.map(line => {
      println("当前线程id：" + Thread.currentThread().getId + ",value: " + line)
      line._1
    })

    result.print().setParallelism(1)

    env.execute("CustormPartitionerDemo")

  }

}
