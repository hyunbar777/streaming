package com.duoduo

import org.apache.flink.streaming.api.scala._

/**
 * Author z
 * Date 2020-06-09 15:24:25
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    
    val stream = env
      .readTextFile(ClassLoader.getSystemClassLoader.getResource("word").getPath)
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()
    
    env.execute()
    
  }
}
