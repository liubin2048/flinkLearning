package com.liubin.flink.dataStream.mySQL

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * author : liubin
  * date : 2019/3/20
  * Description :
  */
case class Account(
                    id: Int,
                    username: String,
                    password: String,
                    age: Int
                  ){}

object MySQLSinkDemo {

  val hostname = "lyytest001"
  val port = 9999

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(classOf.getName)
    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream(hostname, port)

    val data = stream.map(_.split(" ")).filter(_.length == 4)
      .map(fields => {
        Account.apply(fields(0).toInt,fields(1),fields(2),fields(3).toInt)
      })
      .timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction[Account,List[Account],TimeWindow] {
      override def apply(window: TimeWindow, input: Iterable[Account], out: Collector[List[Account]]): Unit = {
        val list = List[Account]()
        if(list.size >0) {
          logger.info("1 分钟内收集到 student 的数据条数是：" + list.size)
          out.collect(list)
        }
      }
    }).addSink(new MySQLSink())




  }

}
