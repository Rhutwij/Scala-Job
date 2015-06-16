package com.jobs2careers.base

import com.typesafe.config.Config
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by carl on 6/9/15.
 */
trait SparkBaseApp extends Serializable{

  def executeTask(params : Array[String]): Unit ={

  }

  def getAppName():String = {
    "defaultJob"
  }

  def getSparkConf(config: Config):SparkConf = {
    val conf = new SparkConf()
    conf.setAppName(getAppName)
    conf.setIfMissing("spark.master", "local[4]")
    conf.setSparkHome(config.getString("spark.context.home"))
    conf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf
  }

}
