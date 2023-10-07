package com.cbp.util

import org.apache.spark.SparkContext

/**
 * @Author changbp
 * @Date 2022-10-23 16:50
 * @Return
 * @Version 1.0
 */
object EnvUtil {
  //ThreadLocal 可以对同一线程的内存进行控制，存储数据、共享数据
  //通过ThreadLocal将sc对象共享到线程内，供其他对象使用
  private val scLocal = new ThreadLocal[SparkContext]()

  //存储
  def put(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  //取出
  def take(): SparkContext = {
    scLocal.get()
  }

  //清除
  def clear(): Unit ={
    scLocal.remove()
  }
}
