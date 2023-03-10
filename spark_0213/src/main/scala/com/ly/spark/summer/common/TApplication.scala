package com.ly.spark.summer.common

import com.ly.spark.summer.util.EnvCache
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  def execute(master: String ="local[*]" ,appName: String)(op: => Unit): Unit = {

    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)

    val sc: SparkContext = new SparkContext(conf)

    EnvCache.put(sc)

    try {
      op
    } catch {
      case e:Exception => e.printStackTrace()
    }

    sc.stop()

    EnvCache.clear()

  }

}
