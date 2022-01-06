package com.xiyou.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xiyou.gmall.realtime.bean.DauInfo
import com.xiyou.gmall.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @author: xy_mono
 * @date: 2021/12/11
 * @description:日活业务实现
 */


object DauAPP {
  def main(args: Array[String]): Unit = {
    //初始化Spark配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    //初始化SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))
    //消费Kafka数据基本实现
    var topic: String = "gmall_start_1122"
    var groupId: String = "gmall_dau_1122"

    //通过SparkStreaming程序从Kafka中读取数据，DStream是指离散化流，是SparkStreaming中的一种抽象表示
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    //此处是表示只选取Kafka数据中的value数据
    //    val jsonDStream: DStream[String] = kafkaDStream.map(_.value())
    //    jsonDStream.print()

    //将日志信息中的时间戳进行处理，转换为日期和小时,因为此时的时间戳显示的是毫秒数
    val jsonObjDStream: DStream[JSONObject] = kafkaDStream.map {
      record => {
        val jsonString: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        //从json对象中获取时间戳
        val ts: lang.Long = jsonObject.getLong("ts")
        //将时间戳转换为日期和小时
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        //将日期和小时分别提取出来，分割后放到json对象中，方便后续处理
        val dateStrArr: Array[String] = dateStr.split(" ")
        var dt = dateStrArr(0)
        var hr = dateStrArr(1)
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject
      }
    }
    //测试输出
    //jsonObjDStream.print(1000)


    //使用Redis进行去重,采用方案：以分区为单位进行过滤，可以减少和连接池交互的次数
    //    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
    //      jsonObjItr => {
    //        //获取Redis客户端，每一个分区获取一次Redis的连接
    //        val jedisClient: Jedis = MyRedisUtil.getJedisClient()
    //
    //        //定义一个集合，用于存放当前分区中的第一次登录的日志
    //        val filteredList = new ListBuffer[JSONObject]()
    //
    //        //对分区的数据进行遍历
    //        for (jsonObject <- jsonObjItr) {
    //          //获取日期
    //          val dt: String = jsonObject.getString("dt")
    //          //获取设备id
    //          val mid: String = jsonObject.getJSONObject("common").getString("mid")
    //          //拼接操作redis的key
    //          var daukey = "dau:" + dt
    //          val isFirst: lang.Long = jedisClient.sadd(daukey, mid)
    //          //设置当天的key数据失效时间为24小时
    //          if (jedisClient.ttl(daukey) < 0) {
    //            jedisClient.expire(daukey, 3600 * 24)
    //          }
    //          if (isFirst == 1L) {
    //            //说明是第一次登录
    //            filteredList.append(jsonObject)
    //          }
    //        }
    //        jedisClient.close()
    //        filteredList.toIterator
    //      }
    //    }

    //通过Redis   对采集到的启动日志进行去重操作  方案2  以分区为单位对数据进行处理，每一个分区获取一次Redis的连接
    //redis 类型 set    key：  dau：2020-10-23    value: mid    expire   3600*24
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      jsonObjItr => {
        //以分区为单位对数据进行处理
        //每一个分区获取一次Redis的连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        //定义一个集合，用于存放当前分区中第一次登陆的日志
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
        //对分区的数据进行遍历
        for (jsonObj <- jsonObjItr) {
          //获取日期
          val dt = jsonObj.getString("dt")
          //获取设备id
          val mid = jsonObj.getJSONObject("common").getString("mid")
          //拼接操作redis的key
          var dauKey = "dau:" + dt
          val isFirst = jedis.sadd(dauKey, mid)
          //设置key的失效时间
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }
          if (isFirst == 1L) {
            //说明是第一次登录
            filteredList.append(jsonObj)
          }
        }
        jedis.close()
        filteredList.toIterator
      }
    }
    //    filteredDStream.count().print()

    //将数据批量的保存到ES中
    filteredDStream.foreachRDD {
      rdd => {
        //以分区为单位对数据进行处理
        rdd.foreachPartition {
          jsonObjItr => {
            //将需要保存到ES中的日活数据以list形式保存
            val dauInfoList: List[(DauInfo)] = jsonObjItr.map {
              jsonObj => {
                //因为我们所需要处理的信息属性在common中
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                //DauInfo是我们之前封装的样例类
                val dauInfo: DauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00",
                  jsonObj.getLong("ts")
                )
                dauInfo
              }
            }.toList
            //将数据批量保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList, "gmall2021_dau_info_" + dt)
          }
        }
      }
    }
    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
