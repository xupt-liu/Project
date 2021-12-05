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
 * @description:����ES�Ŀͻ��˹�����
 */


object MyESUtil {
  //����Jest�ͻ��˹���
  private var jestFactory: JestClientFactory = null

  //�ṩ��ȡJest�ͻ��˵ķ���
  def getJestClient(): JestClient = {
    if (jestFactory == null) {
      //����Jest�ͻ��˹�������
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

  //��һ����ES�в�������,��������Json��ʽ����
  def putindex(): Unit = {
    //��ȡ�ͻ�������
    val jestClient: JestClient = getJestClient()
    //����ִ�е�source
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
    //���������� Index   Builder�еĲ�����ʾҪ���뵽�����е��ĵ����ײ��ת��Json��ʽ���ַ���������Ҳ���Խ��ĵ���װΪ���������
    val index:Index = new Index.Builder(source)
      .index("movie_index_5")
      .`type`("movie")
      .id("1")
      .build()
    //ͨ���ͻ��˶������ES
    jestClient.execute(index)
    //�ر�����
    jestClient.close()
  }

  //��������ES�в������ݣ���������ĵ���װΪһ�����������
  def putIndex2(): Unit ={
    val jestClient = getJestClient()

    //�˴���List��Map����
    val actorList: util.ArrayList[util.Map[String, Any]] = new util.ArrayList[util.Map[String,Any]]()
    val actorMap1: util.HashMap[String, Any] = new util.HashMap[String,Any]()
    actorMap1.put("id",66)
    actorMap1.put("name","����ͮ")
    actorList.add(actorMap1)

    //��װ���������
    val movie: Movie = Movie(300,"�����˲�",9.0f,actorList)
    //����Actionʵ���� ===>Index
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

    //�����ĵ���id,��ES�в�ѯ��һ����¼
  def queryIndexById(): Unit ={
    val jestClient = getJestClient()
    val get:Get = new Get.Builder("movie_index_5","2").build()
    val res:DocumentResult = jestClient.execute(get)
    println(res.getJsonString)
    jestClient.close()
  }

  //����ָ����ѯ��������ES�в�ѯ����ĵ�  ��ʽ1
  def queryIndexByCondition1(): Unit ={
    val jestClient = getJestClient()
    var query:String =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "����"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "����ͮ"}}
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
    //��װSearch����
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index_5")
      .build()
    val res: SearchResult = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = res.getHits(classOf[util.Map[String,Any]])
    //��java��Listת��Ϊjson��List
    import scala.collection.JavaConverters._
    val resList1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList

    println(resList1.mkString("\n"))

    jestClient.close()
  }


  //����ָ����ѯ��������ES�в�ѯ����ĵ�  ��ʽ2
  def queryIndexByCondition2(): Unit ={
    val jestClient = getJestClient()
    //SearchSourceBuilder���ڹ�����ѯ��json��ʽ�ַ���
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder
    val boolQueryBuilder: BoolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name","����"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword","����ͮ"))
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
   * ��ES��������������
   * @param infoList
   * @param indexName
   */
  def bulkInsert(infoList: List[(String,Any)], indexName: String): Unit = {

    if(infoList!=null && infoList.size!= 0){
      //��ȡ�ͻ���
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
      //����������������
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult = jestClient.execute(bulk)
      println("��ES�в���"+bulkResult.getItems.size()+"������")
      jestClient.close()
    }
  }

}
//�˴���Util��Ϊ����Json�������㣬Json��������һ�����϶�java�ģ�������Ҫ����java.util-----������
case class Movie(id:Long,name:String,doubanScore:Float,actorList:util.List[util.Map[String,Any]]){}

