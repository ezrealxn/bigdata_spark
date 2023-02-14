package com.ly.spark.summer.service

import com.ly.spark.pojo.UserCategoryAction
import com.ly.spark.summer.common.TService
import com.ly.spark.summer.dao.HotCategoryTop10Dao
import org.apache.spark.rdd.RDD

class HotCategoryTop10Service extends  TService{

  private val hotCategoryTop10Dao = new HotCategoryTop10Dao

  override def analysis() = {

    val fileDatas = hotCategoryTop10Dao.readFileBySpark("spark_0213/data/user_visit_action.txt")

    // 封装
    val userRDD: RDD[UserCategoryAction] = fileDatas.map(data => {
      val split = data.split("_")

      val date = split(0)
      val userId = split(1)
      val sessionId = split(2)
      val pageId = split(3)
      val actionTime = split(4)
      val searchKeyword = split(5)
      val clickCategoryId = split(6)
      val clickProductId = split(7)
      val orderCategoryIds = split(8)
      val orderProductIds = split(9)
      val payCategoryIds = split(10)
      val payProductIds = split(11)
      val cityId = split(12)

      UserCategoryAction(date,userId,sessionId,pageId,actionTime,searchKeyword,clickCategoryId,clickProductId,orderCategoryIds,
        orderProductIds,payCategoryIds,payProductIds,cityId)
    })


    val flatDatas: RDD[(String, (Int, Int, Int))] = userRDD.flatMap(user => {
      if (user.clickCategoryId != "-1") {
        List((user.clickCategoryId, (1, 0, 0)))
      } else if (user.orderCategoryIds != "null") {
        val ids = user.orderCategoryIds.split(",")
        ids.map(id => {
          (id, (0, 1, 0))
        })
      } else if (user.payCategoryIds != "null") {
        val ids = user.payCategoryIds.split(",")
        ids.map(id => {
          (id, (0, 0, 1))
        })
      } else {44
        Nil
      }
    })

    val resultRDD = flatDatas.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    }).sortBy(_._2, false).take(10)

    resultRDD

  }

}
