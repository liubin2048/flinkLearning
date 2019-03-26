package com.liubin.flink.dataSet.batchAPI

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/13
  * Description : Flink提供了一个分布式缓存，类似于hadoop，可以使用户在并行函数中很方便的读取本地文件
  *
  * 此缓存的工作机制如下：
  * 程序注册一个文件或者目录(本地或者远程文件系统，例如hdfs或者s3)，
  * 通过ExecutionEnvironment注册缓存文件并为它起一个名称。
  * 当程序执行，Flink自动将文件或者目录复制到所有taskmanager节点的本地文件系统，
  * 用户可以通过这个指定的名称查找文件或者目录，然后从taskmanager节点的本地文件系统访问它
  *
  * 1.注册一个文件: env.registerCachedFile("hdfs:///path/to/your/file", "hdfsFile")
  * 2.访问数据: File myFile = getRuntimeContext().getDistributedCache().getFile("hdfsFile");
  */

object DistributedCacheDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // 注册一个文件
    env.registerCachedFile("D:/input.txt", "inputFile")

    val stream = env.fromElements("a", "b", "c", "d")

    val result = stream.map(new RichMapFunction[String, String] {

      override def map(value: String): String = {

        //访问数据
        val file = getRuntimeContext.getDistributedCache.getFile("inputFile")
        value + " , " + file.toString
      }
    }).setParallelism(2)

    result.print()

    env.execute("DistributedCacheDemo")

  }

}
