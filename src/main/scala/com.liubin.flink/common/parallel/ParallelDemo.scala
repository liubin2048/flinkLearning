package com.liubin.flink.common.parallel

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * author : liubin
  * date : 2019/3/19
  * Description : 一个任务的并行度设置可以从多个层次指定
  * - Operator Level（算子层次）
  * - Execution Environment Level（执行环境层次）
  * - Client Level（客户端层次）
  * - System Level（系统层次）
  */
object ParallelDemo {

  import org.apache.flink.api.scala._

  // 并行度设置之Operator Level
  // 一个算子、数据源和sink的并行度可以通过调用 setParallelism()方法来指定
  val env1 = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1 = env1.socketTextStream("lyytest001",9999)
  val wordCounts1 = stream1
    .flatMap(_.split("\\s"))
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1)
    .setParallelism(5)


  // 并行度设置之Execution Environment Level
  // 执行环境(任务)的默认并行度可以通过调用setParallelism()方法指定。为了以并行度3来执行所有的算子、数据源和data sink， 可以通过如下的方式设置执行环境的并行度：
  // 执行环境的并行度可以通过显式设置算子的并行度而被重写
  val env2 = StreamExecutionEnvironment.getExecutionEnvironment
  env2.setParallelism(3)

  val stream2 = env2.socketTextStream("lyytest001",9999)
  val wordCounts = stream2
    .flatMap(_.split("\\s"))
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1)


  // 并行度设置之Client Level
  // 并行度可以在客户端将job提交到Flink时设定。对于CLI客户端，可以通过-p参数指定并行度
  // ./bin/flink run -p 10 WordCount-java.jar


  // 并行度设置之System Level
  // 在系统级可以通过设置flink-conf.yaml文件中的parallelism.default属性来指定所有执行环境的默认并行度

}
