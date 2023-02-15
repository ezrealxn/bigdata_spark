package com.ly.spark.summer.service

import com.ly.spark.summer.bean.UserCategoryAction
import com.ly.spark.summer.common.TService
import com.ly.spark.summer.dao.HotCategoryTop10SessionDao
import org.apache.spark.rdd.RDD

class HotCategoryTop10SessionService extends  TService{


  private val hotCategoryTop10SessionDao: HotCategoryTop10SessionDao = new HotCategoryTop10SessionDao


  override def analysis(data: Any) = {

    val top10Ids: Array[String] = data.asInstanceOf[Array[String]]

    val fileDatas = hotCategoryTop10SessionDao.readFileBySpark("spark_0213/data/user_visit_action.txt")

    // 封装
    val userRDD: RDD[UserCategoryAction] = fileDatas.map(data => {
      val split = data.split("_")

      val date = split(0)
      val userId = split(1).toLong
      val sessionId = split(2)
      val pageId = split(3).toLong
      val actionTime = split(4)
      val searchKeyword = split(5)
      val clickCategoryId = split(6).toLong
      val clickProductId = split(7).toLong
      val orderCategoryIds = split(8)
      val orderProductIds = split(9)
      val payCategoryIds = split(10)
      val payProductIds = split(11)
      val cityId = split(12).toLong

      UserCategoryAction(date,userId,sessionId,pageId,actionTime,searchKeyword,clickCategoryId,clickProductId,orderCategoryIds,
        orderProductIds,payCategoryIds,payProductIds,cityId)
    })


    val clickRDD: RDD[UserCategoryAction] = userRDD.filter {
      data => {
        if (data.clickCategoryId != -1) {
          top10Ids.contains(data.clickCategoryId.toString)
        } else {
          false
        }
      }
    }



    val reduceRDD: RDD[((Long, String), Int)] = clickRDD.map(data => {
      ((data.clickCategoryId, data.sessionId), 1)
    }).reduceByKey(_ + _)


    val groupRDD: RDD[(Long, Iterable[(String, Int)])] = reduceRDD.map {
      case ((cid, sid), cnt) => {
        (cid, (sid, cnt))
      }
    }.groupByKey()



    groupRDD.mapValues(item =>{
      item.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    }).collect()


  }

}
