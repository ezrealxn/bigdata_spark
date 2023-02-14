package com.ly.spark.req

import com.ly.spark.pojo.UserCategoryAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReqHotCategoryTop10 {
  def main(args: Array[String]): Unit = {

    // TODO TOP10热门商品
    // TODO spark配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ReqHotCategoryTop10")
    // TODO spark上下文对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // TODO 点击数据

    val fileDatas: RDD[String] = sc.textFile("spark_0213/data/user_visit_action.txt")
    // 封装
    val userRdd: RDD[UserCategoryAction] = fileDatas.map(data => {
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
    // 过滤
    val userClickRdd: RDD[UserCategoryAction] = userRdd.filter(_.clickCategoryId !="-1")

    // 计算点击数据
    val clickCntDatas: RDD[(String, Int)] = userClickRdd.map(user=> (user.clickCategoryId,1)).reduceByKey(_+_)


    // TODO 下单数据
    // 过滤
    val userOrderRdd: RDD[UserCategoryAction] = userRdd.filter(_.orderCategoryIds !="null")
    // 计算
    val orderCntDatas: RDD[(String, Int)] = userOrderRdd.flatMap(users => {
      val ids = users.orderCategoryIds
      val cids = ids.split(",")
      cids.map((_, 1))
    }).reduceByKey(_ + _)

    // TODO 支付数据的计算

    // 过滤
    val userPayRDD: RDD[UserCategoryAction] = userRdd.filter(_.payCategoryIds!="null")

    val payCntDatas: RDD[(String, Int)] = userPayRDD.flatMap(pays => {
      val ids = pays.payCategoryIds.split(",")
      ids.map((_, 1))
    }).reduceByKey(_ + _)


//    val clickSortedDatas: RDD[(String, Int)] = clickCntData.sortBy(_._2, false)
    /**
    *val cidCntDatas: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCntData.cogroup(orderCntDatas,payCntDatas)
 **
 *
 val joinDatas: RDD[(String, (Int, Int, Int))] = cidCntDatas.map {
      *case (cid, (clickIter, orderIter, payIter)) => {
 **
 var clickCnt = 0
        *var orderCnt = 0
        *var payCnt = 0
 **
 val iterator1: Iterator[Int] = clickIter.iterator
 **
 val iterator2: Iterator[Int] = orderIter.iterator
 **
 val iterator3: Iterator[Int] = payIter.iterator
 **
 if (iterator1.hasNext) {
          *clickCnt = iterator1.next()
        *}
 **
 if (iterator2.hasNext) {
          *orderCnt = iterator2.next()
        *}
 **
 if (iterator3.hasNext) {
          *payCnt = iterator3.next()
        *}
 **
 (cid, (clickCnt, orderCnt, payCnt))
      *}
    *}
 *
 **/

    val clickMapDatas: RDD[(String, (Int, Int, Int))] = clickCntDatas.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }


    val orderMapDatas: RDD[(String, (Int, Int, Int))] = orderCntDatas.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }


    val payMapDatas: RDD[(String, (Int, Int, Int))] = payCntDatas.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }


    val unionMapDatas: RDD[(String, (Int, Int, Int))] = clickMapDatas.union(orderMapDatas).union(payMapDatas)


    val resultRDD: RDD[(String, (Int, Int, Int))] = unionMapDatas.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val result: Array[(String, (Int, Int, Int))] = resultRDD.sortBy(_._2,false).take(10)


    result.foreach(println)


    sc.stop()

  }
}
