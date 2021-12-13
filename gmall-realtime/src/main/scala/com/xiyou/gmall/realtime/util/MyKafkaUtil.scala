package com.xiyou.gmall.realtime.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * Author: xy_mono
 * Date: 2020/12/10
 * Desc: 读取kafka的工具类
 */

object MyKafkaUtil {
  val broker_list = properties.getProperty("kafka.broker.list")
//  val broker_list = properties.getProperty("kafka.broker.list")
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  // kafka消费者配置,此处是一个scala的可变集合
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall1122_group",
    //latest自动重置偏移量为最新的偏移量，此处可以不设置，这是默认的
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量，为了实现精准一次消费
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )


  // 创建DStream，返回接收到的输入数据   使用默认的消费者组，这里设置了kafka的消费策略
  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    dStream
  }

  //在对Kafka数据进行消费的时候，指定消费者组
  def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam("group.id") = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    dStream
  }

  //从指定的偏移量位置读取数据
  def getKafkaStream(topic: String, ssc: StreamingContext, offsets: Map[TopicPartition, Long], groupId: String)
  : InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam("group.id") = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets))
    dStream
  }

}
