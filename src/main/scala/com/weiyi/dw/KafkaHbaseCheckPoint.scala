package com.weiyi.dw

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by xp on 2018/8/26.
  */
object KafkaHbaseCheckPoint {
  Logger.getLogger("org").setLevel(Level.WARN)
  // Hbase 简要配置以及开启服务

  val hbaseConf =  HBaseConfiguration.create()

  hbaseConf.set("hbase.zookeeper.quorum", "linux01:2181,linux02:2181,linux03:2181")
  val connHbase = ConnectionFactory.createConnection(hbaseConf)
  val admin = connHbase.getAdmin()


  // 确认 Hbase 表存在
  def ensureHbaseTBExsit(topic:String) = {

    val tableName = TableName.valueOf("kafka_offSet")
    val isExist = admin.tableExists(tableName)

    // 是否存在表，不存在新建
    if (!isExist) {
      val htable = new HTableDescriptor(tableName)

      // topic 为 ColumnFamily
      htable.addFamily(new HColumnDescriptor(topic))
      admin.createTable(htable)
      println("表创建成功:" + htable)
    }
  }

  // 保存新的 OffSet
  def storeOffSet(ranges: Array[OffsetRange], topic:Array[String]) = {

    val table = new HTable(hbaseConf, "kafka_offSet")
    table.setAutoFlush(false, false)

    ensureHbaseTBExsit(topic(0).toString)

    val putList =new java.util.ArrayList[Put]()

    for(o <- ranges){
      val rddTopic = o.topic
      val rddPartition = o.partition
      val rddOffSet = o.untilOffset
      println("topic:" + rddTopic + ",    partition:" + rddPartition + ",    offset:" + rddOffSet)


      val put = new Put(Bytes.toBytes("kafka_offSet_" + o.topic))
      // ColumnFamily
      put.add(Bytes.toBytes(o.topic), Bytes.toBytes(o.partition), Bytes.toBytes(o.untilOffset))

      putList.add(put)
    }
    table.put(putList)
    table.flushCommits()
    println("保存新 offset 成功！")
  }


  // 得到历史 OffSet
  def getOffset(topic: Array[String])={

    val topics = topic(0).toString
    val fromOffSets = scala.collection.mutable.Map[TopicPartition, Long]()

    ensureHbaseTBExsit(topics)

    val table = new HTable(hbaseConf, "kafka_offSet")
    val rs = table.getScanner(new Scan())

    // 获取数据  每条数据的列名为partition，值为offset
    for (r:Result <- rs.next(10)) {
      for (kv:KeyValue <- r.raw()) {
        val partition = Bytes.toInt(kv.getQualifier)
        val offSet = Bytes.toLong(kv.getValue)
        println("获取到的partition:" + partition + ",   opffset:" + offSet)
        fromOffSets.put(new TopicPartition(topics, partition), offSet)
      }
    }

    // 返回值
    if (fromOffSets.isEmpty){
      (fromOffSets.toMap, 0)
    } else {
      (fromOffSets.toMap, 1)
    }

  }

  // 创建 DStream
  def createMyStreamingContextHbase(ssc:StreamingContext, topic:Array[String],
                                    kafkaParams:Map[String, Object]):InputDStream[ConsumerRecord[String, String]]= {

    var kafkaStreams:InputDStream[ConsumerRecord[String, String]] = null
    val (fromOffSet, flag) = getOffset(topic)

    println("获取到的Offset：" + fromOffSet)

    if (flag == 1) {
      kafkaStreams = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topic, kafkaParams, fromOffSet))
    } else {
      kafkaStreams = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topic, kafkaParams))
    }

    kafkaStreams
  }

  def main(args: Array[String]): Unit = {

    // spark streaming 配置
    val conf = new SparkConf().setAppName("offSet Hbase").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka 配置
    val brokers = "linux01:9092,linux02:9092,linux03:9092"
    val topics = Array("zkKafka")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "zk_group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )


    createMyStreamingContextHbase(ssc, topics, kafkaParams)

      .foreachRDD(rdds => {

        if(!rdds.isEmpty()) {

          println("##################:" + rdds.count())
        }

        //  保存新的 Offset
        storeOffSet(rdds.asInstanceOf[HasOffsetRanges].offsetRanges, topics)

      })

    // 启动
    ssc.start()
    ssc.awaitTermination()

  }

}