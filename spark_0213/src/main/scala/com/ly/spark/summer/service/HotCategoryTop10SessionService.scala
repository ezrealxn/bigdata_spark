package com.ly.spark.summer.service

import com.ly.spark.pojo.UserCategoryAction
import com.ly.spark.summer.common.TService
import com.ly.spark.summer.dao.{HotCategoryTop10Dao, HotCategoryTop10SessionDao}

class HotCategoryTop10SessionService extends  TService{

  private val hotCategoryTop10Dao = new HotCategoryTop10Dao

  private val hotCategoryTop10SessionDao: HotCategoryTop10SessionDao = new HotCategoryTop10SessionDao


  override def analysis() = {


  }

}
