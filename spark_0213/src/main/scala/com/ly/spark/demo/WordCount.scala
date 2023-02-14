package com.ly.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WordCount{
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val spark: SparkContext = new SparkContext(sparkConf)

    val lines: RDD[String] = spark.textFile("spark_0213/data/word.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    val wordcount = wordGroup.mapValues(_.size)

    wordcount.collect().foreach(println)

    spark.stop()
  }
}