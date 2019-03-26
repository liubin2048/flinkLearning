package com.liubin.flink.common.stateManagement

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/11
  * Description : 状态管理statebackend功能相关配置
  */

object StateBackendDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置状态管理statebackend(全局设置)
    /*
       修改flink-conf.yaml
       state.backend: filesystem
       state.checkpoints.dir: hdfs://namenode:9000/flink/checkpoints
       注意：state.backend的值可以是下面几种：
         jobmanager(代表MemoryStateBackend)
         filesystem(代表FsStateBackend)
         rocksdb(代表RocksDBStateBackend)
    */


    // 设置状态管理statebackend(单任务调整)

    //  1.MemoryStateBackend(不建议使用)
    //    state数据保存在java堆内存中，执行checkpoint的时候，会把state的快照数据保存到jobmanager的内存中
    env.setStateBackend(new MemoryStateBackend())

    //  2.FsStateBackend(HDFS)
    //    state数据保存在taskmanager的内存中，执行checkpoint的时候，会把state的快照数据保存到配置的文件系统中
    env.setStateBackend(new FsStateBackend("hdfs://hadoop001:9000/flink/checkpoints"))

    //  3.RocksDBStateBackend(建议使用)【需要添加第三方依赖】
    //    RocksDB跟上面的都略有不同，它会在本地文件系统中维护状态，state会直接写入本地rocksdb中。
    //    同时它需要配置一个远端的filesystem uri（一般是HDFS），在做checkpoint的时候，会把本地的数据直接复制到filesystem中。
    //    fail over的时候从filesystem中恢复到本地
    //    RocksDB克服了state受内存限制的缺点，同时又能够持久化到远端文件系统中，比较适合在生产中使用
    env.setStateBackend(new RocksDBStateBackend(
      "hdfs://hadoop001:9000/flink/checkpoints", true))

  }
}
