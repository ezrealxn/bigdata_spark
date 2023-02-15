package com.ly.spark.req

import com.ly.spark.summer.bean.UserCategoryAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReqPageFlow_1 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ReqHotCategoryTop10_1")
    // TODO spark上下文对象
    val sc: SparkContext = new SparkContext(sparkConf)

    val fileDatas: RDD[String] = sc.textFile("spark_0213/data/user_visit_action.txt")
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

    userRDD.cache()
    // TODO 页面跳转的转化率统计只要固定id

    val okIds: List[Int] = List(1,2,3,4,5,6,7)

    val okFlowIds: List[(Int, Int)] = okIds.zip(okIds.tail)


    val result = userRDD.filter(
      user =>{
        okIds.init.contains(user.pageId.toInt)
      }
    ).map(user => {
      (user.pageId, 1)
    }).reduceByKey(_ + _).collect().toMap


    val groupRDD: RDD[(String, Iterable[UserCategoryAction])] = userRDD.groupBy(_.sessionId)


    val mapRDD = groupRDD.mapValues(iter => {
      val actions: List[UserCategoryAction] = iter.toList.sortBy(_.actionTime)

      val ids: List[Int] = actions.map(_.pageId.toInt)

      val flowIds = ids.zip(ids.tail)
      flowIds.filter(
        ids =>{
          okFlowIds.contains(ids)
        }
      )
    })

    val mapRDD2 = mapRDD.map(_._2)

    val flatRDD= mapRDD2.flatMap(list => list)


    val reduceRDD = flatRDD.map((_,1)).reduceByKey(_+_)


    reduceRDD.foreach{
      case ((id1 , id2 ) , cnt)=>{
        println(s"页面 【${id1}-> ${id2}】单跳页面转化率为： "+(cnt.toDouble  / result.getOrElse(id1, 1)) )
      }
    }


    sc.stop()

  }
}
