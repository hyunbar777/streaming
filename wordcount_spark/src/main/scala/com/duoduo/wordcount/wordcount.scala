package com.duoduo.wordcount

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.duoduo.utils.{MyESUtil, MyKafkaUtil_008, MyKafkaUtil_010, PropertiesUtil, RedisUtil}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis


/**
 * Author z
 * Date 2020-05-25 10:44:58
 */
object wordcount {
  val log = LoggerFactory.getLogger(classOf[Student])
  
  def main(args: Array[String]): Unit = {
    
    PropertiesUtil.setPath(args(0))
    print(args(0))
    print(PropertiesUtil.getProperty("kafka.broker.list"))
    val conf = new SparkConf()
      .setAppName("wordcount")
    /*      .setMaster("local[*]")*/
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ck1")
    ssc.sparkContext.setLogLevel("warn")
    //1.获取数据，从kakfa中
    val dstream1 = MyKafkaUtil_010.getDStream(ssc, "mytest")
    //2.转换数据类型为studnet
    val stream_log = dstream1
      .map {
        //case log => JSON.parseObject(log.value(), classOf[Student])
        case log =>print(log); log.value()
      }
    stream_log.print(100)
    stream_log.saveAsTextFiles("/data/demo.txt")
  
     stream_log.foreachRDD(rdd=>{
       rdd.foreachPartition(it=>{
         val slist = it.toList
         log.info(slist.toString())
         
         //4.保存在es
         MyESUtil.insertBulk("index_student",slist)
       })
     })
    // val key = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    //3.对数据去重
    /* val filterDStream = stream_log.transform(rdd => {
       val distinctRDD = rdd.groupBy(_.id)
         .flatMap {
           case (_, it) => it.take(1)
         }
       distinctRDD.collect.foreach(print)
       //从redis中读取清单过滤
       val client = RedisUtil.getJedisClient
 
       //获取到redis清单，每个周期获取一次
       val uids = client.smembers(key)
       //把得到的uids进行广播，否则在其他executor上无法得到这个变量的值
       val uidsBD = ssc.sparkContext.broadcast(uids)
       client.close()
       distinctRDD.filter(log => !uidsBD.value.contains(log.id))
     })*/
    /*    filterDStream.foreachRDD(rdd=>{
          rdd.foreachPartition(it=>{
            val client: Jedis = RedisUtil.getJedisClient
            val slist = it.toList
            slist.foreach(s=>{
              client.sadd(key,s.id)
            })
            client.close()
            //4.保存在es
            MyESUtil.insertBulk("index_student",slist)
          })
        })*/
  
    ssc.start()
    ssc.awaitTermination()
  }
}

case class Student(id: String, name: String, age: Int)