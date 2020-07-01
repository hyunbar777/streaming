package com.duoduo.utils.hotitems

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * Author z
 * Date 2020-05-14 09:45:42
 */
class kafkaUtils {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    
    val stream = env
        .addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
}
