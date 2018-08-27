package com.weiyi.dw

import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
  * Created by xp on 2018/8/26.
  */
object KafkaProducer {
  def main(args: Array[String]) {
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers","linux01:9092")
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)

    //创建一个生产者实例
    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](props)
    var i =0

    while (i <= 100000000) {

      val partition = i % 3

      // 0 - 1
      val index = Random.nextInt(8) + 'a'
      //partiton > key > 轮询
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("zkKafka",partition,"",String.valueOf(index.toChar))
      // 发送消息
      producer.send(record)
      Thread.sleep(300)
      println(record)
      i += 1
    }
  }
}
