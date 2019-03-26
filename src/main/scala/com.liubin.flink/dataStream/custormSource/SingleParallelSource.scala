package com.liubin.flink.dataStream.custormSource

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * author : liubin
  * date : 2019/3/12
  * Description : 自定义单并行度source，生成从1开始递增的数字
  */
class SingleParallelSource extends SourceFunction[Long] {

  var number = 1L
  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while(isRunning){
      ctx.collect(number)
      number += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

}
