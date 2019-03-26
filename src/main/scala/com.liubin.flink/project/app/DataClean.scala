package com.liubin.flink.project.app

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.bonc.bdev.project.constant.PropertiesConstants
import com.bonc.bdev.project.util.RedisSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * author : liubin
  * date : 2019/3/15
  * Description : 数据清洗【实时ETL】 针对算法产生的日志数据进行清洗拆分
  * 1：算法产生的日志数据是嵌套json格式，需要拆分打平
  * 2：针对算法中的国家字段进行大区转换
  * 3：最后把不同类型的日志数据分别进行存储
  */
object DataClean {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 修改并行度
    env.setParallelism(5)
    // 设置流处理时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(5000)
    // 设置checkpoint模式为exactly once（默认值）
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // 设置statebackend
    // env.setStateBackend(new RocksDBStateBackend("hdfs://lyytest001:9000/flink/checkpoints",true))

    // 隐式转换
    import org.apache.flink.api.scala._

    // 创建kafka consumer
    val props = new Properties()
    props.setProperty("bootstrap.servers",PropertiesConstants.KAFKA_BROKERS)
    props.setProperty("group.id",PropertiesConstants.KAFKA_GROUP_ID)
    props.setProperty("auto.offset.reset",PropertiesConstants.KAFKA_AUTO_OFFSET_RESET)
    val consumer = new FlinkKafkaConsumer011[String](
      PropertiesConstants.DATA_CLEAN_SOURCE_TOPIC,
      new SimpleStringSchema(),
      props
    )

    // 获取kafka中的数据
    //{"dt":"2019-01-01 11:11:11","countryCode":"US","data":[{"type":"s1","score":0.3,"level":"A"},{"type":"s2","score":0.1,"level":"B"}]}
    val data = env.addSource(consumer)

    // 最新的国家码和大区的映射关系，将数据发送到后面transformation算子的所有并行实例中
    val mapData = env.addSource(new RedisSource).broadcast

    val resData = data.connect(mapData).flatMap(new CoFlatMapFunction[String,mutable.Map[String,String],String] {

      // 存储国家和大区的映射关系
      var allMap = mutable.Map[String,String]()

      // flatMap1处理的是kafka中的数据
      override def flatMap1(value: String, out: Collector[String]): Unit = {

        val jsonObjects = JSON.parseObject(value)
        val dt = jsonObjects.getString("dt")
        val countryCode = jsonObjects.getString("countryCode")

        // 获取大区
        val area = allMap.get(countryCode)

        val jsonArray = jsonObjects.getJSONArray("data")
        for(i <- 0 until jsonArray.size()) {
          val jsonObject = jsonArray.getJSONObject(i)
          jsonObject.put("area",area)
          jsonObject.put("dt",dt)
          out.collect(jsonObject.toString)
        }
      }

      // flatMap2 处理的是redis返回的map类型的数据
      override def flatMap2(value: mutable.Map[String, String], out: Collector[String]): Unit = {
        this.allMap = value
      }
    })

    val outprops = new Properties()
    outprops.setProperty("bootstrap.servers",PropertiesConstants.KAFKA_BROKERS)
    outprops.setProperty("transaction.timeout.ms", 60000 * 15 + "")
    // 第一种方案，设置FlinkKafkaProducer011里面的事务超时时间
    // prop.setProperty("transaction.timeout.ms",60000*15+"")
    //第二种方案，设置kafka的最大事务超时时间

    val producer = new FlinkKafkaProducer011[String](
      PropertiesConstants.DATA_CLEAN_SINK_TOPIC,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema),
      outprops,
      FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)
    resData.addSink(producer)

    env.execute("dataClean")

  }

}
