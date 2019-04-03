package com.liubin.flink.dataSet.batchAPI

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/26
  * Description : 从两个数据集中创建一个包含该键的元素列表的元组。要指定连接键，必须使用“where”和“isEqualTo”方法。
  */
object CoGroupDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val authors = env.fromElements(
      Tuple3("A001", "zhangsan", "zhangsan@qq.com"),
      Tuple3("A001", "lisi", "lisi@qq.com"),
      Tuple3("A001", "wangwu", "wangwu@qq.com")
    )
    val posts = env.fromElements(
      Tuple2("P001", "zhangsan"),
      Tuple2("P002", "lisi"),
      Tuple2("P003", "wangwu"),
      Tuple2("P004", "lisi")
    )

    // scala中coGroup没有with方法来使用CoGroupFunction
    val result: CoGroupDataSet[(String, String, String), (String, String)] = authors.coGroup(posts).where(1).equalTo(1)

    result.collect().foreach(line => {

      for (a <- line._1; b <- line._2) {
        print(a)
        println(b)
      }

    })

  }

}
