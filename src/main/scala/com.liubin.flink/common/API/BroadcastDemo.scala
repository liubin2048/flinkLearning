package com.liubin.flink.common.API

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

/**
  * author : liubin
  * date : 2019/3/13
  * Description : broadcast 广播变量
  *
  * 1.初始化数据: DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3)
  * 2.广播数据:  .withBroadcastSet(toBroadcast, "broadcastSetName");
  * 3.获取数据:  Collection<Integer> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
  */

object BroadcastDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    // 准备需要广播的数据
    val broadData = ListBuffer[Tuple2[String, Int]]()
    broadData.append(("la", 22))
    broadData.append(("lb", 23))
    broadData.append(("lc", 24))

    // 处理需要广播的数据
    val tupleData = env.fromCollection(broadData)

    val broadcastData = tupleData.map(tup => {
      Map(tup._1 -> tup._2)
    })

    val text = env.fromElements("la", "lb", "lc")

    val result = text.map(new RichMapFunction[String, String] {


      var listData: java.util.List[Map[String, Int]] = null
      var allMap = Map[String, Int]()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        this.listData = getRuntimeContext.getBroadcastVariable[Map[String, Int]]("broadcastMapName")
        val it = listData.iterator()
        while (it.hasNext) {
          val next = it.next()
          allMap = allMap.++(next)
        }
      }

      override def map(value: String) = {
        val age = allMap.get(value).get
        value + "," + age
      }
    }).withBroadcastSet(broadcastData, "broadcastMapName")

    result.print()

  }

}
