package com.cbp.application

import com.cbp.common.TApplication
import com.cbp.controller.WordCountController

/**
 * @Author changbp
 * @Date 2022-10-23 16:21
 * @Return
 * @Version 1.0
 * app层，应用程序的起始，负责程序的环境的创建，及调度层的调用
 */
object WordCountApp extends App with TApplication {
  private val wordCountController = new WordCountController
  //启动程序
  start("local[*]", "WordCount") {
    //通过Controller调度
    wordCountController.dispatch()
  }
}
