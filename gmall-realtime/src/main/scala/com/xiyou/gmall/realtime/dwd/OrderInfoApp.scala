package com.xiyou.gmall.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xiyou.gmall.realtime.bean.OrderInfo
import com.xiyou.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author: xy_mono
 * @date: 2022/3/12
 * @description:判断是否为用户首单实现
 */


object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    //1.从 Kafka 中查询订单信息
    val sparkConf: SparkConf = new
        SparkConf().setMaster("local[4]").setAppName("OrderInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_info"
    val groupId = "order_info_group"

    //从 Redis 中读取 Kafka 偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] =
      OffsetManagerUtil.getOffset(topic, groupId)
    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      //Redis 中有偏移量 根据 Redis 中保存的偏移量读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      // Redis 中没有保存偏移量 Kafka 默认从最新读取
      recordDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //得到本批次中处理数据的分区对应的偏移量起始及结束位置
    // 注意：这里我们从 Kafka 中读取数据之后，直接就获取了偏移量的位置，因为 KafkaRDD 可以转换为 HasOffsetRanges，会自动记录位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] =
      recordDstream.transform {
        rdd => {
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }

    //对从 Kafka 中读取到的数据进行结构转换，由 Kafka 的 ConsumerRecord 转换为一个OrderInfo 对象
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        //通过对创建时间 2020-07-13 01:38:16 进行拆分，赋值给日期和小时属性，方便后续处理
        val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
        //获取日期赋给日期属性
        orderInfo.create_date = createTimeArr(0)
        //获取小时赋给小时属性
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }

    //方案 2：对 DStream 中的数据进行处理，判断下单的用户是否为首单
    //优化:以分区为单位，将一个分区的查询操作改为一条 SQL
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] =
    orderInfoDStream.mapPartitions {
      orderInfoItr => {
        //因为迭代器迭代之后就获取不到数据了，所以将迭代器转换为集合进行操作
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区内的用户 ids
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //从 hbase 中查询整个分区的用户是否消费过，获取消费过的用户 ids
        var sql: String = s"select user_id,if_consumed from user_status2020 where user_id in('${userIdList.mkString("','")}') "
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //得到已消费过的用户的 id 集合
        val cosumedUserIdList: List[String] =
          userStatusList.map(_.getString("USER_ID"))
        //对分区数据进行遍历
        for (orderInfo <- orderInfoList) {
          //注意：orderInfo 中 user_id 是 Long 类型，一定别忘了进行转换
          if (cosumedUserIdList.contains(orderInfo.user_id.toString)) {
            //如已消费过的用户的 id 集合包含当前下订单的用户，说明不是首单
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }
    orderInfoWithFirstFlagDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
