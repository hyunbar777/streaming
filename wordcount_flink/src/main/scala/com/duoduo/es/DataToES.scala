package com.duoduo.es

import java.util

import com.duoduo.utils.{KafkaProperties, PropertiesUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author z
 * Date 2020-05-22 09:53:36
 */
object DataToES {
  val log = LoggerFactory.getLogger(classOf[Student])
  def main(args: Array[String]): Unit = {
//    val p = ParameterTool.fromArgs(args)
//    val host = p.get("host")
//    val port = p.getInt("port")
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost(PropertiesUtil.getProperty("es.host.01"),PropertiesUtil.getProperty("es.port.01").toInt,"http"))
    httpHosts.add(new HttpHost(PropertiesUtil.getProperty("es.host.02"),PropertiesUtil.getProperty("es.port.02").toInt,"http"))
    httpHosts.add(new HttpHost(PropertiesUtil.getProperty("es.host.03"),PropertiesUtil.getProperty("es.port.03").toInt,"http"))
    
    //log.debug(httpHosts.get(0).toString)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //开启checkPoint, Time interval between state checkpoints 5000 milliseconds.
    /**
     * 如果我们启用了Flink的Checkpint机制，
     * 那么Flink Kafka Consumer将会从指定的Topic中消费消息，
     * 然后定期地将Kafka offsets信息、状态信息以及其他的操作信息进行Checkpint。
     * 所以，如果Flink作业出故障了，Flink将会从最新的Checkpint中恢复，
     * 并且从上一次偏移量开始读取Kafka中消费消息。
     */
    env.enableCheckpointing(1000)
    env.setParallelism(1)
    
    val dataStream = env
      .addSource(
        new FlinkKafkaConsumer011[String](PropertiesUtil.getProperty("kafka.topic_01"), new SimpleStringSchema, KafkaProperties.geKafkaProperties())
      )
      .map(data => {
        log.info(data)
        val dataArray = data.split(",")
        Student(dataArray(0), dataArray(1), dataArray(2).toInt)
        
      })
  
    val esSinkBuilder = new ElasticsearchSink.Builder[Student](
      httpHosts,
      new ElasticsearchSinkFunction[Student] {
        override def process(t: Student, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          print("数据：" + t)
          val json = new util.HashMap[String, String]()
          json.put("id", t.id)
          json.put("name", t.name)
          json.put("age", t.age.toString)
          log.info(json.toString)
          val indexRequest = Requests
            .indexRequest
            .index("student")
            .`type`("data")
            .source(json)
          requestIndexer.add(indexRequest)
          print("保存成功")
        }
      })
    dataStream.addSink(esSinkBuilder.build)
  }
}


