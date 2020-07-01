package com.duoduo.utils

import java.io.{FileInputStream, InputStream}
import java.util.Properties

/**
 * Author z
 * Date 2020-05-25 16:03:52
 */
object PropertiesUtil {
 // private val inputStream: InputStream =ClassLoader.getSystemResourceAsStream("config.properties")
  private val properties = new Properties()

  
  def setPath(path:String): Unit ={
    val in = new FileInputStream(path)
    properties.load(in)
  }
  def getProperty(propertyName:String)={
    properties.getProperty(propertyName)
  }
}
