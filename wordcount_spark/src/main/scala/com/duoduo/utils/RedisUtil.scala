package com.duoduo.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}


/**
 * Author z
 * Date 2020-05-25 15:56:38
 */
object RedisUtil {
  private val host: String = PropertiesUtil.getProperty("redis.host")
  private val port = PropertiesUtil.getProperty("redis.port").toInt
  
  private val config = new JedisPoolConfig()
  //最大连接数
  config.setMaxTotal(100)
  //最大空闲
  config.setMaxIdle(20)
  //最小空闲
  config.setMinIdle(20)
  //忙碌时是否等待
  config.setBlockWhenExhausted(true)
  //忙碌时等待时长
  config.setMaxWaitMillis(500)
  //每次获得连接的进行测试
  config.setTestOnBorrow(false)
  private val pool = new JedisPool(config,host,port)
  
  def getJedisClient={
    pool.getResource
  }
}
