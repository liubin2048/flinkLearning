package com.liubin.flink.dataStream.custormSource

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
  * author : liubin
  * date : 2019/3/12
  * Description : 自定义多并行度的source，生成从1开始递增的数字
  */
class ParallelSource extends RichParallelSourceFunction[Long] {

  var number = 1L
  var isRunning = true

  override def run(ctx: SourceContext[Long]) = {
    while(isRunning){
      ctx.collect(number)
      number+=1
      Thread.sleep(1000)
    }

  }

  override def cancel() = {
    isRunning = false
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

}
