package com.cbp.common

import com.cbp.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * @Author changbp
 * @Date 2022-10-23 16:45
 * @Return
 * @Version 1.0
 * dao持久层，负责数据的交互，获取数据、修改数据、写入数据
 */
trait TDao {
  def readFile(path: String): RDD[String] = {
    // 读取文件数据
    EnvUtil.take().textFile(path)
  }
}
