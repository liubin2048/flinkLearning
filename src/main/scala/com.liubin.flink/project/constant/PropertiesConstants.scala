package com.liubin.flink.project.constant

/**
  * author : liubin
  * date : 2019/3/15
  * Description : 
  */
object PropertiesConstants {

  // 并行度parallel
  val STREAM_PARALLELISMS = 4
  val SINK_PARALLELISMS = 1

  // kafka
  val KAFKA_GROUP_ID = "flinkProject" + System.currentTimeMillis()
  val KAFKA_AUTO_OFFSET_RESET = "latest"
  val KAFKA_BROKERS = "lyytest001:9092,lyytest002:9092,lyytest003:9092"
  val KAFKA_ZOOKEEPER_CONNECT = "lyytest001:2181,lyytest002:2181,lyytest003:2181/kafka011"

  val DATA_CLEAN_SOURCE_TOPIC = "allData"
  val DATA_CLEAN_SINK_TOPIC = "allDataClean"

  val DATA_REPORT_SOURCE_TOPIC = "auditLog"
  val DATA_REPORT_SINK_TOPIC = "metriesTopic"
  val DATA_REPORT_LATE_DATA_TOPIC = "lateLog"

  // redis
  val REDIS_PORT = 6379
  val REDIS_HOST = "lyytest001"

  // es
  val ES_PORT = 9200
  val ES_HOST = "lyytest001"
  val ES_SCHEME = "http"

}
