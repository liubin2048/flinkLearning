package com.liubin.flink.dataStream.HBase

import java.util
import java.util.{Date, Properties}

import org.apache.commons.net.ntp.TimeStamp
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * author : liubin
  * date : 2019/3/19
  * Description : 
  */
object HBaseSinkDemo {

  // kafka conf
  val topicName = "flinkHbaseSinkTest"
  val brokers = "lyytest001:9092,lyytest002:9092,lyytest003:9092"
  val groupId = "flinkHbaseSinkTest"

  // hbase conf
  val tableName = TableName.valueOf("flinkTest")
  val columnFamily = "cf"
  val zookeeper = "lyytest001:2181,lyytest002:2181,lyytest003:2181"
  val znodeParent = "/hbase"


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置flink App并行度
    env.setParallelism(1)
    // 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(5000)
    // 设置流处理时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //添加scala隐式转行
    import org.apache.flink.api.scala._

    // kafka source
    val props = new Properties()
    props.setProperty("bootstrap.servers", brokers)
    props.setProperty("group.id", groupId)
    props.setProperty("auto.offset.reset", "latest")
    val consumer = new FlinkKafkaConsumer011[String](topicName, new SimpleStringSchema(), props)

    // hbase sink
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", zookeeper)
    hconf.set("hbase.zookeeper.znode.parent", znodeParent)
    hconf.setInt("hbase.rpc.timeout", 30000)
    hconf.setInt("hbase.client.operation.timeout", 30000)
    hconf.setInt("hbase.client.scanner.timeout.period", 30000)

    var hConnect: Connection = null
    var hTable: Table = null

    val stream = env.addSource(consumer)
    stream.map(line => {

      hConnect = ConnectionFactory.createConnection(hconf)
      hTable = hConnect.getTable(tableName)

      //rowkey
      val ts = new TimeStamp(new Date())
      val date = ts.getDate
      val put = new Put(Bytes.toBytes(date.getTime))

      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("test"), Bytes.toBytes(line))

      // ------------------方式1------------------
      hTable.put(put)
      // ------------------方式2------------------
      /*
      val hList = new util.ArrayList[Put]() //设置缓存List
      hList.add(put)
      val params = new BufferedMutatorParams(tableName) //设置hbase缓存区
      params.writeBufferSize(1024 * 1024) //缓存区的大小为1m,当达到1m时数据会自动刷到hbase
      val mutator = hConnect.getBufferedMutator(params)
      mutator.mutate(hList)
      mutator.flush()
      hList.clear()
      */

      if (hTable != null) {
        hTable.close()
      }
      if (hConnect != null) {
        hConnect.close()
      }

    })

    env.execute("HBaseSinkDemo")

  }

}
