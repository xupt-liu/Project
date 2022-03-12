package com.xiyou.gmall.realtime.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

/**
 * @author: xy_mono
 * @date: 2022/3/6
 * @description:用于向Kafka中发送数据
 */


object MyKafkaSink {
  val broker_list = properties.getProperty("kafka.broker.list")
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  var kafkaProducer: KafkaProducer[String, String] = null

  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  def send(topic: String, key: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  def createKafkaProducer: KafkaProducer[String, String] = {
    val properties = new Properties
    properties.put("bootstrap.servers", broker_list)
    properties.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idempotence", (true: java.lang.Boolean))
    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }


}
