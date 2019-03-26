package com.liubin.flink.project.util

import com.bonc.bdev.project.constant.PropertiesConstants
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable

/**
  * author : liubin
  * date : 2019/3/15
  * Description : Redis Source
  */

class RedisSource extends SourceFunction[mutable.Map[String, String]] {

  val logger = LoggerFactory.getLogger(getClass.getName)

  val SLEEP_TIME = 60000

  var isRunning = true

  var jedis: Jedis = _

  override def run(ctx: SourceFunction.SourceContext[mutable.Map[String, String]]): Unit = {

    // 连接Redis
    this.jedis = new Jedis(PropertiesConstants.REDIS_HOST, PropertiesConstants.REDIS_PORT)
    // 隐式转换，把java的hashmap转为scala的map
    import scala.collection.JavaConversions.mapAsScalaMap

    // 存储所有国家和大区的对应关系
    var keyValueMap = mutable.Map[String, String]()

    while (isRunning) {
      try {
        keyValueMap.clear()
        keyValueMap = jedis.hgetAll("areas")

        for (key <- keyValueMap.keys.toList) {
          val values = keyValueMap.get(key).get
          val splits = values.split(",")
          for (value <- splits) {
            keyValueMap += (key -> value)
          }
        }

        if (keyValueMap.nonEmpty) {
          ctx.collect(keyValueMap)
        } else {
          logger.warn("redis is null")
        }

        Thread.sleep(SLEEP_TIME)
      } catch {
        case e: JedisConnectionException => {
          logger.error("redis connection exception : ", e.getCause)
          logger.error("Try reconnecting to the redis !")
          jedis = new Jedis(PropertiesConstants.REDIS_HOST, PropertiesConstants.REDIS_PORT)
        }
        case e: Exception => {
          logger.error("Execption : ",e.getCause)
        }
      }
    }
  }

  override def cancel(): Unit = {
    isRunning = false
    if(jedis != null) {
      jedis.close()
    }
  }
}
