package com.liubin.flink.dataStream.redis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * author : liubin
  * date : 2019/3/12
  * Description : 接收socket数据，把数据保存到redis中
  */
object RedisSinkDemo {

  // 自定义类继承RedisMapper，并实现其中的方法
  class RedisSinkMapperDemo extends RedisMapper[Tuple2[String,String]] {

    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }
    override def getKeyFromData(data: (String, String)): String = {
      data._1
    }
    override def getValueFromData(data: (String, String)): String = {
      data._2
    }
  }

  def main(args: Array[String]): Unit = {

    val hostname = "lyytest001"
    val redisPort = 6379
    val socketPort = 9999

    // 创建redis的配置
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost(hostname)
      .setPort(redisPort)
      .build()

    // 创建redis sink
    val redisSink = new RedisSink[Tuple2[String,String]](conf,new RedisSinkMapperDemo)

    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置flink App并行度
    env.setParallelism(1)
    // 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(5000)
    // 设置流处理时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //添加scala隐式转行
    import org.apache.flink.api.scala._

    //链接socket获取输入数据
    val stream = env.socketTextStream(hostname,socketPort)

    //对数据进行组装,把string转化为tuple2<String,String>
    val data = stream.map(line => ("flink",line))

    data.addSink(redisSink)

    env.execute("RedisSinkDemo")
  }

}
