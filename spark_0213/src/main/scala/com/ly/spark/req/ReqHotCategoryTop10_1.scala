package com.ly.spark.req


import com.ly.spark.pojo.UserCategoryAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReqHotCategoryTop10_1 {
  def main(args: Array[String]): Unit = {

    // TODO TOP10热门商品
    // TODO spark配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ReqHotCategoryTop10_1")
    // TODO spark上下文对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // TODO 点击数据

    val fileDatas: RDD[String] = sc.textFile("spark_0213/data/user_visit_action.txt")
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

    val resultRDD: RDD[(String, (Int, Int, Int))] = flatDatas.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })

    val result = resultRDD.sortBy(_._2, false).take(10)

    result.foreach(println)

    sc.stop()

  }
}
