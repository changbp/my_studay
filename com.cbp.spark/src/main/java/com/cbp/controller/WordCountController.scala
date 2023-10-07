package com.cbp.controller

import com.cbp.common.TController
import com.cbp.service.WordCountService

/**
 * @Author changbp
 * @Date 2022-10-23 16:02
 * @Return
 * @Version 1.0
 * 调度层，只负责程序调度及打印，不实现具体逻辑
 */
class WordCountController extends TController {
  private val wordCountService = new WordCountService

  override def dispatch(): Unit = {
    //调用service层
    val word2Count = wordCountService.dataAnalysis()
    // 打印结果
    word2Count.foreach(println)
  }
}
