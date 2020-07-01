package com.duoduo.utils

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Author z
 * Date 2020-05-25 09:54:42
 */
object MyKafkaUtil_008 {
  val brokers = PropertiesUtil.getProperty("kafka.broker.list")
  val group =  PropertiesUtil.getProperty("kafka.group")
  val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
  
  val kafkaParam = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> group,
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
  )

  def getDStream(ssc: StreamingContext, topic: String) = {
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      kafkaParam,
      Set(topic)
    )
  }
}
