package com.jobs2careers.base

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

trait SparkLocalConfig {
  private val config = ConfigFactory.load()
  val sparkConf = new SparkConf()
  
  sparkConf.setAppName(this.getClass.getName)
  sparkConf.setIfMissing("spark.master", "local[*]")
  sparkConf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
}
