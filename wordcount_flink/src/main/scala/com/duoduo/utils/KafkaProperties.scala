package com.duoduo.utils

import java.util.Properties

/**
 * Author z
 * Date 2020-05-22 10:25:28
 */
object KafkaProperties {
  private val properties = new Properties()
  properties.setProperty("bootstrap.servers", PropertiesUtil.getProperty("bootstrap.servers"))
  properties.setProperty("group.id", PropertiesUtil.getProperty("group.id"))
  properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("auto.offset.reset", PropertiesUtil.getProperty("auto.offset.reset"))
  
  def geKafkaProperties() ={
    properties
  }

}
