package com.duoduo.es

import com.duoduo.utils.PropertiesUtil

/**
 * Author z
 * Date 2020-05-26 16:26:59
 */
object mytest {
  def main(args: Array[String]): Unit = {
    
    print(PropertiesUtil.getProperty("es.host.01"))
    print(PropertiesUtil.getProperty("es.port.01"))
  }
}
