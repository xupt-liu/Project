package com.xiyou.gmall.realtime

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import org.elasticsearch.search.builder.SearchSourceBuilder
import io.searchbox.core._
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

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
    var source:String = {
    """{
      |  "id":300,
      |  "name":"incident red sea",
      |  "doubanScore":5.0,
      |  "actorList":[
      |{"id":4,"name":"zhang san feng"}
      |]
      |}""".stripMargin

    }
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

    //此处是List和Map方法
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
      .id("2")
      .build()
    jestClient.execute(index)
    jestClient.close()
  }

  def main(args: Array[String]): Unit = {
    queryIndexById()
  }

    //根据文档的id,从ES中查询出一条记录
  def queryIndexById(): Unit ={
    val jestClient = getJestClient()
    val get:Get = new Get.Builder("movie_index_5","2").build()
    val res:DocumentResult = jestClient.execute(get)
    println(res.getJsonString)
    jestClient.close()
  }

  //根据指定查询条件，从ES中查询多个文档  方式1
  def queryIndexByCondition1(): Unit ={
    val jestClient = getJestClient()
    var query:String =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "天龙"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "李若彤"}}
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
      """.stripMargin
    //封装Search对象
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index_5")
      .build()
    val res: SearchResult = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = res.getHits(classOf[util.Map[String,Any]])
    //将java的List转换为json的List
    import scala.collection.JavaConverters._
    val resList1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList

    println(resList1.mkString("\n"))

    jestClient.close()
  }


  //根据指定查询条件，从ES中查询多个文档  方式2
  def queryIndexByCondition2(): Unit ={
    val jestClient = getJestClient()
    //SearchSourceBuilder用于构建查询的json格式字符串
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder
    val boolQueryBuilder: BoolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name","天龙"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword","李若彤"))
    searchSourceBuilder.query(boolQueryBuilder)
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(10)
    searchSourceBuilder.sort("doubanScore",SortOrder.ASC)
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))
    val query: String = searchSourceBuilder.toString
    //println(query)

    val search: Search = new Search.Builder(query).addIndex("movie_index_5").build()
    val res: SearchResult = jestClient.execute(search)
    val resList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = res.getHits(classOf[util.Map[String,Any]])

    import scala.collection.JavaConverters._
    val list = resList.asScala.map(_.source).toList
    println(list.mkString("\n"))

    jestClient.close()
  }

  def main(args: Array[String]): Unit = {
    queryIndexByCondition2()
  }

  /**
   * 向ES中批量插入数据
   * @param infoList
   * @param indexName
   */
  def bulkInsert(infoList: List[(String,Any)], indexName: String): Unit = {

    if(infoList!=null && infoList.size!= 0){
      //获取客户端
      val jestClient = getJestClient()
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
      for ((id,dauInfo) <- infoList) {
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          .id(id)
          .`type`("_doc")
          .build()
        bulkBuilder.addAction(index)
      }
      //创建批量操作对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult = jestClient.execute(bulk)
      println("向ES中插入"+bulkResult.getItems.size()+"条数据")
      jestClient.close()
    }
  }

}
//此处的Util是为了让Json解析方便，Json解析工具一般是认定java的，所以需要导入java.util-----样例类
case class Movie(id:Long,name:String,doubanScore:Float,actorList:util.List[util.Map[String,Any]]){}

