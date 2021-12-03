package com.xiyou.gmall.realtime

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}

/**
 * @author: xy_mono
 * @date: 2021/12/3
 * @description:操作ES的客户端工具类
 */


object MyESUtil {
  //声明Jest客户端工厂
  private var jestFactory: JestClientFactory = null

  //提供获取Jest客户端的方法
  def getJestClient(): JestClient = {
    if (jestFactory == null) {
      //创建Jest客户端工厂对象
      build();
    }
    jestFactory.getObject
  }

  def build(): Unit = {
    jestFactory = new JestClientFactory
    jestFactory.setHttpClientConfig(new HttpClientConfig
    .Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000).build())
  }

  //法一：向ES中插入数据,将数据以Json形式发送
  def putindex(): Unit = {
    //获取客户端连接
    val jestClient: JestClient = getJestClient()
    //定义执行的source
    var source:String =
    """{
      |  "id":300,
      |  "name":"incident red sea",
      |  "doubanScore":5.0,
      |  "actorList":[
      |{"id":4,"name":"zhang san feng"}
      |]
      |}""".stripMargin
    //创建插入类 Index   Builder中的参数表示要插入到索引中的文档，底层会转换Json格式的字符串，所以也可以将文档封装为样例类对象
    val index:Index = new Index.Builder(source)
      .index("movie_index_5")
      .`type`("movie")
      .id("1")
      .build()
    //通过客户端对象操作ES
    jestClient.execute(index)
    //关闭连接
    jestClient.close()
  }

  //法二：向ES中插入数据，将插入的文档封装为一个样例类对象
  def putIndex2(): Unit ={
    val jestClient = getJestClient()

    val actorList: util.ArrayList[util.Map[String, Any]] = new util.ArrayList[util.Map[String,Any]]()
    val actorMap1: util.HashMap[String, Any] = new util.HashMap[String,Any]()
    actorMap1.put("id",66)
    actorMap1.put("name","李若彤")
    actorList.add(actorMap1)

    //封装样例类对象
    val movie: Movie = Movie(300,"天龙八部",9.0f,actorList)
    //创建Action实现类 ===>Index
    val index: Index = new Index.Builder(movie)
      .index("movie_index_5")
      .`type`("movie")
      .id("2").build()
    jestClient.execute(index)
    jestClient.close()
  }

}


