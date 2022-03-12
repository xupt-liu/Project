package com.xiyou.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.xiyou.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: xy_mono
 * @date: 2022/3/6
 * @description:从Kafka中读取数据，根据表名进行分流
 */


object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //消费Kafka数据基本实现
    var topic: String = "gmall_db_c"
    var groupId: String = "gmall_db_canal_group"

    //从 Redis 中读取偏移量
    var recoredDStream: InputDStream[ConsumerRecord[String, String]] = null
    val kafkaOffsetMap: Map[TopicPartition, Long] =
      OffsetManagerUtil.getOffset(topic, groupId)
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      recoredDStream =
        MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      recoredDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //获取当前采集周期中处理的数据 对应的分区已经偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] =
      recoredDStream.transform {
        rdd => {
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }

    //将从 kafka 中读取到的 recore 数据进行封装为 json 对象
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //获取 value 部分的 json 字符串
        val jsonStr: String = record.value()
        //将 json 格式字符串转换为 json 对象
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    }

    //从 json 对象中获取 table 和 data，发送到不同的 kafka 主题
    jsonObjDStream.foreachRDD {
      rdd => {
        rdd.foreach {
          jsonObj => {
            //获取更新的表名
            val tableName: String = jsonObj.getString("table")
            //获取当前对表数据的更新
            val dataArr: JSONArray = jsonObj.getJSONArray("data")
            val opType: String = jsonObj.getString("type")
            //拼接发送的主题
            var sendTopic = "ods_" + tableName
            import scala.collection.JavaConverters._
            if ("INSERT".equals(opType)) {
              for (data <- dataArr.asScala) {
                val msg: String = data.toString
                //向 kafka 发送消息
                MyKafkaSink.send(sendTopic, msg)
              }
            }
          }
        }
        //修改 Redis 中 Kafka 的偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
