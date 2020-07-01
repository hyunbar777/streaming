package com.duoduo.utils

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * Author z
 * Date 2020-05-25 09:54:42
 */
object MyKafkaUtil_010 {
  
  val kafkaParam = Map(
    "bootstrap.servers" -> PropertiesUtil.getProperty("kafka.broker.list"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> PropertiesUtil.getProperty("kafka.group"),
    "auto.offset.reset" -> "latest",
    //true,消费者的偏移量会在后台自动提交，但是kafka宕机后容易丢失数据
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )
  
  /*
  创建Dstream，返回接收到的输入数据
  LocationStrategies：根据给定的主题和集群地址创建consumer
   LocationStrategies.PreferConsistent：持续的在所有executor之间分配分区
   ConsumerStrategies：选择如何在Driver和Executor上创建和配置kafka Consumer
    ConsumerStrategies.Subscribe：订阅一系列主题
   */
  def getDStream(ssc: StreamingContext, topic: String) = {
    KafkaUtils.createDirectStream[String, String](
      ssc,
      // 标配. 只要 kafka 和 spark 没有部署在一台设备就应该是这个参数
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
  }
}
