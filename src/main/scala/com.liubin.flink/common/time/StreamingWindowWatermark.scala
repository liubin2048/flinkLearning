package com.liubin.flink.common.time

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
  * author : liubin
  * date : 2019/3/12
  * Description : 在窗口统计中使用watermark
  */
object StreamingWindowWatermark {

  def main(args: Array[String]): Unit = {

    val hostname = "lyytest001"
    val port = 9999

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.api.scala._

    val socketStream = env.socketTextStream(hostname, port)

    val inputMap = socketStream.map(line => {
      val fields = line.split(",")
      (fields(0), fields(1).toLong)
    })

    val watermarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {

      var currentMaxTimestamp = 0L
      var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        val id = Thread.currentThread().getId
        println("currentThreadId:" + id + ",key:" + element._1 + ",eventtime:[" + element._2 + "|" + sdf.format(element._2) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp + "|" + sdf.format(getCurrentWatermark().getTimestamp) + "]")

        timestamp
      }
    })

    val window = watermarkStream.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3))) //按照消息的EventTime分配窗口，和调用TimeWindow效果一样
      .apply(new WindowFunction[Tuple2[String, Long], String, Tuple, TimeWindow] {
      override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
        val keyStr = key.toString
        val arrBuffer = ArrayBuffer[Long]()
        val iterator = input.iterator
        while(iterator.hasNext){
          val tup2 = iterator.next()
          arrBuffer.append(tup2._2)
        }

        val arr = arrBuffer.toArray
        Sorting.quickSort(arr)

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
        val result = keyStr + "," + arr.length + "," + sdf.format(arr.head) + "," + sdf.format(arr.last)+ "," + sdf.format(window.getStart) + "," + sdf.format(window.getEnd)
        out.collect(result)
      }
    })

    window.print()

    env.execute("StreamingWindowWatermark")

  }

}
