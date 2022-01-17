package com.xiyou.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util

/**
 * @author: xy_mono
 * @date: 2022/1/8
 * @description:维护偏移量的工具类
 */


object OffsetManagerUtil {
  //实现功能一：获取offset
  /**
   * 从Redis中获取偏移量
   * Reids 格式：type=>Hash
   * [key=>offset:topic:groupId   field=>partitionId    value=>偏移量值] expire 不需要指定
   * 这里需要注意getOffset的返回值类型，即是Map。
   */
  def getOffset(topicName: String, groupId: String): Map[TopicPartition, Long] = {
    //获取Redis客户端
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    //拼接key
    val offsetKey: String = "offset:" + topicName + ":" + groupId
    //根据key从Redis中获取数据
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    //关闭Redis客户端
    jedis.close()

    //因为我们要求的返回值类型是Map[TopicPartition,Long]，但是目前的返回值只是Map[String, String]
    //将java中的map转换成scala中的map,这个API是经常使用的。
    import scala.collection.JavaConverters._
    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      //此处是元组，此时根据hsetall获得的元组是partitionId和offset，将一个元组转换为另一个要求的元组
      case (partitionId, offset) => {
        println("读取分区偏移量：" + partitionId + ":" + offset)
        //将 Redis 中保存的分区对应的偏移量进行封装
        (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
      }
    }.toMap
    kafkaOffsetMap
  }

  //实现功能二：保存offset
  def saveOffset(topicName: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    //定义Java的map集合，用于向Redis中保存数据
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    //对封装的偏移量数组offsetRanges进行遍历
    for (offset <- offsetRanges) {
      //获取分区
      val partition: Int = offset.partition
      //获取结束点
      val untilOffset: Long = offset.untilOffset
      //封装到Map集合中
      offsetMap.put(partition.toString, untilOffset.toString)
      //打印测试
      println("保存分区：" + partition + ":" + offset.fromOffset + "--->" + offset.untilOffset)
    }
    //拼接Redis中存储的key
    val offsetKey: String = "offset:" + topicName + ":" + groupId
    //如果需要保存的偏移量不为空 执行保存操作
    if (offsetMap != null && offsetMap.size() > 0) {
      //获取Redis客户端
      val jedis: Jedis = MyRedisUtil.getJedisClient()
      //保存到 Redis 中
      jedis.hmset(offsetKey, offsetMap)
      //关闭Redis客户端
      jedis.close()
    }
  }
}
