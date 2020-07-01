package com.duoduo.utils

import java.util

import com.duoduo.wordcount.Student
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}
import org.apache.http.HttpHost

/**
 * Author z
 * Date 2020-05-25 11:00:54
 */
object MyESUtil {
  
  private val factory: JestClientFactory = new JestClientFactory
  
  //客户端工厂

    val config = new HttpClientConfig
    .Builder("http://10.40.0.160:9200")
      .multiThreaded(true)
      .maxTotalConnection(100)
      .connTimeout(10000)
      .readTimeout(10000)
      .build()
    factory.setHttpClientConfig(config)
  
  //获取客户端对象
  def getClient = {
    factory.getObject
  }
  
  //关闭客户端对象
  def closeClient(c: JestClient) = {
    
    if (c != null) {
      try {
        c.close
      } catch {
        case e => e.printStackTrace()
      }
    }
  }
  def insertSingle(index:String,source:Any): Unit ={
    if (index == null || source == null) return
  
    val client = getClient
    val single = new Index.Builder(source)
      .`type`("_doc")
      .index(index)
      .build()
    client.execute(single)
    closeClient(client)
  }
  
  /**
   * 批量插入数据到es中
   *
   * @param index
   * @param sources 数据源
   *                insertSingle("user", User("a", 20))
   *                insertBulk("user", Iterator(User("aa", 20), User("bb", 30)))
   *                insertBulk("user", Iterator((1,User("aa", 20)), (2,User("bb", 30))))
   */
  def insertBulk(index: String, sources: Iterable[Any]): Unit = {
    if (index == null || sources == null  || sources.isEmpty) return
    val bulkBuilder = new Bulk
    .Builder()
      .defaultIndex(index)
      .defaultType("_doc")
    //把所有的source变成action添加buck中
    sources.foreach({
      //传入的值是元祖，第一个表示id
      case (id: String, data) => bulkBuilder.addAction(
        new Index.Builder(data).id(id).build()
      )
      //其他没有id的类型，将来会自动生成默认id
      case data => bulkBuilder.addAction(new Index.Builder(data).build())
    })
    val client = getClient
    
    client.execute(bulkBuilder.build())
    closeClient(client)
  }
  
  def main(args: Array[String]): Unit = {
    insertBulk("student",Student("1", "zs", 1)::Nil)
  }
}
