package com.liubin.flink.common.API

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * author : liubin
  * date : 2019/3/13
  * Description :  一个具体的累加器Accumulator实现：Counter(IntCounter、LongCounter、DoubleCounter)
  *
  * 1.创建累加器:  private IntCounter numLines = new IntCounter();
  * 2.注册累加器:  getRuntimeContext().addAccumulator("num-lines", this.numLines);
  * 3.使用累加器:  this.numLines.add(1);
  * 4.获取累加器的结果: myJobExecutionResult.getAccumulatorResult("num-lines")
  */

object AccumulatorsDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data = env.fromElements("a", "b", "c", "d")

    val res = data.map(new RichMapFunction[String, String] {

      // 定义累加器
      val numLines = new IntCounter

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        // 注册累加器
        getRuntimeContext.addAccumulator("numLines", this.numLines)
      }

      override def map(value: String): String = {
        // 使用累加器
        this.numLines.add(1)
        value
      }
    }).setParallelism(4)

    res.writeAsText("D:/AccumulatorResult")
    val jobResult = env.execute("AccumulatorsDemo")

    // 获取累加器的结果
    val num = jobResult.getAccumulatorResult[Int]("numLines")
    println("AccumulatorResult (numLines) :" + num)

  }

}
