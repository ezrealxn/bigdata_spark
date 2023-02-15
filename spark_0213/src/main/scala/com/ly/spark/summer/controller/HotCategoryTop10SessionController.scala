package com.ly.spark.summer.controller

import com.ly.spark.summer.common.TController
import com.ly.spark.summer.service.{HotCategoryTop10Service, HotCategoryTop10SessionService}

class HotCategoryTop10SessionController extends TController{

  private val hotCategoryTop10Service = new HotCategoryTop10Service

  private val hotCategoryTop10SessionService: HotCategoryTop10SessionService = new HotCategoryTop10SessionService

  override def dispatch(): Unit = {

    val top10: Array[(String, (Int, Int, Int))] = hotCategoryTop10Service.analysis()

    val result= hotCategoryTop10SessionService.analysis(top10.map(_._1))

    result.foreach(println)

  }
}
