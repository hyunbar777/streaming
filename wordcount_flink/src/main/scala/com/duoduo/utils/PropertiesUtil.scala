package com.duoduo.utils

import java.io.InputStream
import java.util.Properties

/**
 * Author z
 * Date 2020-05-25 16:03:52
 */
object PropertiesUtil {
  private val inputStream: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
  private val properties = new Properties()
  properties.load(inputStream)
  def getProperty(propertyName:String)=properties.getProperty(propertyName)
}
