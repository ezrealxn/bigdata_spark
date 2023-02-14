package com.ly.spark.summer.common

import com.ly.spark.summer.util.EnvCache
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable
import scala.io.{BufferedSource, Source}

trait TDao {

  def readFile(path:String ): List[String] ={

    val source: BufferedSource = Source.fromFile(EnvCache.get() + path)

    val lines: List[String] = source.getLines().toList

    source.close()

    lines

  }

  def readFileBySpark(path:String): RDD[String] ={
    EnvCache.get().asInstanceOf[SparkContext].textFile(path)
  }

}
