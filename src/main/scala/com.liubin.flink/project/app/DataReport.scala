package com.liubin.flink.project.app

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import com.bonc.bdev.project.constant.PropertiesConstants
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.{Tuple, Tuple4}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.macros.ParseException
import scala.util.Sorting

/**
  * author : liubin
  * date : 2019/3/18
  * Description : 数据报表 (主要针对直播/短视频平台审核指标的统计)
  * 1：统计不同大区每1 min内过审(上架)的数据量
  * 2：统计不同大区每1 min内未过审(下架)的数据量
  * 3：统计不同大区每1 min内加黑名单的数据量
  */
object DataReport {

  val logger = LoggerFactory.getLogger(getClass.getName)

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

    //隐式转换
    import org.apache.flink.api.scala._

    // 创建kafka consumer
    val props = new Properties
    props.setProperty("bootstrap.servers", PropertiesConstants.KAFKA_BROKERS)
    props.setProperty("group.id", PropertiesConstants.KAFKA_GROUP_ID)
    props.setProperty("auto.offset.reset", PropertiesConstants.KAFKA_AUTO_OFFSET_RESET)
    val consumer = new FlinkKafkaConsumer011[String](
      PropertiesConstants.DATA_REPORT_SOURCE_TOPIC,
      new SimpleStringSchema(),
      props
    )

    // 获取kafka中的数据
    val data = env.addSource(consumer)

    // 对数据进行清洗
    val mapData = data.map(line => {
      val jsonObject = JSON.parseObject(line)
      val dt = jsonObject.getString("dt")
      var time = 0L

      try {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val parse = sdf.parse(dt)
        time = parse.getTime
      } catch {
        case e: ParseException => {
          logger.error("时间解析异常，dt:" + dt, e.getCause)
        }
      }

      val type1 = jsonObject.getString("type")
      val area = jsonObject.getString("area")

      (time, type1, area)

    })

    //过滤掉异常数据
    val filterData = mapData.filter(_._1 > 0)

    // 保存迟到太久的数据
    // 注意：针对java代码需要引入org.apache.flink.util.OutputTag
    //      针对scala代码 需要引入org.apache.flink.streaming.api.scala.OutputTag
    val outputTag = new OutputTag[Tuple3[Long, String, String]]("late-data") {}

    val resultData = filterData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, String)] {

      var currentMaxTimestamp = 0L
      var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s

      override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

      override def extractTimestamp(element: (Long, String, String), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1, 2)
      .window(TumblingEventTimeWindows.of(Time.seconds(30)))
      .allowedLateness(Time.seconds(30)) //允许迟到30s
      .sideOutputLateData(outputTag)
      .apply(new WindowFunction[Tuple3[Long, String, String], Tuple4[String, String, String, Long], Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, String)], out: Collector[Tuple4[String, String, String, Long]]) = {
          //获取分组字段信息
          val type1 = key.getField(0).toString
          val area = key.getField(1).toString
          val it = input.iterator
          //存储时间，为了获取最后一条数据的时间
          val arrBuf = ArrayBuffer[Long]()
          var count = 0
          while (it.hasNext) {
            val next = it.next
            arrBuf.append(next._1)
            count += 1
          }
          println(Thread.currentThread.getId + ",window触发了，数据条数：" + count)
          //排序
          val arr = arrBuf.toArray
          Sorting.quickSort(arr)

          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val time = sdf.format(new Date(arr.last))
          //组装结果
          val res = new Tuple4[String, String, String, Long](time, type1, area, count)
          out.collect(res)
        }
      })

    //获取迟到太久的数据
    val sideOutput = resultData.getSideOutput[Tuple3[Long, String, String]](outputTag)

    //把迟到的数据存储到kafka中
    val outprops = new Properties()
    outprops.setProperty("bootstrap.servers", PropertiesConstants.KAFKA_BROKERS)
    outprops.setProperty("transaction.timeout.ms", 60000 * 15 + "")

    val producer = new FlinkKafkaProducer011[String](
      PropertiesConstants.DATA_REPORT_LATE_DATA_TOPIC,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      outprops,
      FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)

    sideOutput.map(tup => tup._1 + "\t" + tup._2 + "\t" + tup._3).addSink(producer)


    //把计算的结果存储到es中
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost(PropertiesConstants.ES_HOST, PropertiesConstants.ES_PORT, PropertiesConstants.ES_SCHEME))

    val esSinkBuilder = new ElasticsearchSink.Builder[Tuple4[String, String, String, Long]](
      httpHosts,
      new ElasticsearchSinkFunction[Tuple4[String, String, String, Long]] {
        def createIndexRequest(element: Tuple4[String, String, String, Long]): IndexRequest = {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element.f0)
          json.put("type", element.f1)
          json.put("area", element.f2)
          json.put("count", element.f3)

          val id = element.f0.replace(" ", "_") + "-" + element.f1 + "-" + element.f2

          return Requests.indexRequest()
            .index("auditindex")
            .`type`("audittype")
            .id(id)
            .source(json)
        }

        override def process(element: Tuple4[String, String, String, Long], runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
          requestIndexer.add(createIndexRequest(element))
        }
      }
    )

    esSinkBuilder.setBulkFlushMaxActions(1)

    resultData.addSink(esSinkBuilder.build())

    env.execute("dataReport")


  }

}
