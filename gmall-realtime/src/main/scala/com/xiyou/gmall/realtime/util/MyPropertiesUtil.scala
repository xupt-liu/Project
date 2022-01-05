package com.xiyou.gmall.realtime.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * Author: xy_mono
 * Date: 2020/12/10
 * Desc: 读取配置文件的工具类
 */
object MyPropertiesUtil {
  //  def main(args: Array[String]): Unit = {
  //    val prop: Properties = MyPropertiesUtil.load("config.properties")
  //    println(prop.getProperty("kafka.broker.list"))
  //  }

  def load(propertiesName: String): Properties = {
    val prop: Properties = new Properties()
    //加载指定的配置文件
    prop.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8))
    prop
  }
}
