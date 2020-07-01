package com.duoduo.utils

import com.duoduo.wordcount.Student
import io.searchbox.client.JestClient

/**
 * Author z
 * Date 2020-05-25 14:22:29
 */
object MyESTest {
  def main(args: Array[String]): Unit = {
    val client = MyESUtil.getClient
    mutiOperation(client)
  }
  def mutiOperation(client:JestClient)={
    val s1 = Student("1","zs",20)
    val s2 = Student("2","ls",40)
    MyESUtil.insertBulk("student",Iterable(s1,s2))
    MyESUtil.insertBulk("student",Iterable((1,s1),(2,s2)))
  }
}
