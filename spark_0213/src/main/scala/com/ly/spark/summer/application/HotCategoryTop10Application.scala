package com.ly.spark.summer.application

import com.ly.spark.summer.common.TApplication
import com.ly.spark.summer.controller.HotCategoryTop10Controller

object HotCategoryTop10Application extends TApplication with App {

  execute(appName = "HotCategoryTo10"){
    val controller: HotCategoryTop10Controller = new HotCategoryTop10Controller
    controller.dispatch()
  }

}
