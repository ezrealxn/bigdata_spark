package com.ly.spark.summer.bean

case class UserCategoryAction(
  date:String,  // 行为日期
  userId:Long,  //用户id
  sessionId:String,  //session的id
  pageId:Long,  //页面id
  actionTime:String,   //行为时间
  searchKeyword:String, //搜索关键字
  clickCategoryId:Long, //品类的id
  clickProductId:Long,    //商品的id
  orderCategoryIds:String, //订单品类ID的集合
  orderProductIds:String, //订单产品ID的集合
  payCategoryIds:String,  // 支付品类id的集合
  payProductIds:String,  //支付商品id的集合
  cityId:Long   // 城市ID
)
