package com.liubin.flink.dataStream.ElasticSearch

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * author : liubin
  * date : 2019/3/14
  * Description : ElasticSearch Sink
  */
object ElasticSearchSinkDemo {

  def main(args: Array[String]): Unit = {

    val socketHostname = "lyytest001"
    val socketPort = 9999

    // es sink
    val esPort = 9200
    val esHost = "lyytest001"
    val esScheme = "http"

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost(esHost, esPort, esScheme))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        def createIndexRequest(element: String): IndexRequest = {
          val json = new java.util.HashMap[String, String]
          json.put("data", element)

          // 可根据id值覆盖操作
          val id = ""

          return Requests.indexRequest()
            .index("my-index")
            .`type`("my-type")
            .id(id)
            .source(json)
        }

        override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          indexer.add(createIndexRequest(element))
        }
      }
    )

    // 批量请求的配置;这指示接收器在每个元素之后发出，否则它们将被缓冲
    esSinkBuilder.setBulkFlushMaxActions(1)
    // 为内部创建的REST客户机上的自定义配置提供RestClientFactory
    //esSinkBuilder.setRestClientFactory(resClientBuilder => {
      //resClientBuilder.setDefaultHeaders()
      //resClientBuilder.setMaxRetryTimeoutMillis()
      //resClientBuilder.setPathPrefix("")
      //resClientBuilder.setHttpClientConfigCallback()
    //})

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置flink App并行度
    env.setParallelism(1)
    // 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(5000)
    // 设置流处理时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 链接socket获取输入数据
    val stream = env.socketTextStream(socketHostname, socketPort)
    // 添加ES输出
    stream.addSink(esSinkBuilder.build()).setParallelism(1)

    env.execute("ElasticSearchSinkDemo")

  }

}
