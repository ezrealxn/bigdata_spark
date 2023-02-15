package com.ly.spark.summer.application

import com.ly.spark.summer.common.TApplication
import com.ly.spark.summer.controller.HotCategoryTop10SessionController

object HotCategoryTop10SessionApplication extends TApplication with App {

  execute(appName = "HotCategoryTo10Session"){
    val controller: HotCategoryTop10SessionController = new HotCategoryTop10SessionController
    controller.dispatch()
  }

}
