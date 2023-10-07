package com.cbp.service

import com.cbp.common.TService
import com.cbp.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @Author changbp
 * @Date 2022-10-23 16:01
 * @Return
 * @Version 1.0
 *  service层，负责程序的具体实现
 */
class WordCountService extends TService {
  private val wordCountDao = new WordCountDao

  override def dataAnalysis(): Array[(String, Int)] = {
    //调用dao层获取数据
    val path = "com.cbp.spark/input/word.txt"
    val fileRDD: RDD[String] = wordCountDao.readFile(path)
    // 数据分词
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    // 转换数据结构 word => (word, 1)
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    // 将转换结构后的数据按照相同的单词进行分组聚合
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_ + _)
    // 将数据聚合结果采集到内存中
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()
    word2Count
  }
}
