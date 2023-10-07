package com.cbp.common

import com.cbp.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author changbp
 * @Date 2022-10-23 16:27
 * @Return
 * @Version 1.0
 * @Disciption 提取特质
 */
trait TApplication {
  //可以传一段代码逻辑到参数中
  def start(master: String, appName: String)(op: => Unit): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
    // 创建 Spark 上下文环境对象（连接对象)
    val sc: SparkContext = new SparkContext(sparkConf)
    EnvUtil.put(sc)
    //捕获异常
    try {
      op
    } catch {
      case ex: Exception => print(ex.getMessage)
    }
    //关闭 Spark 连接
    sc.stop()
    EnvUtil.clear()
  }

}
