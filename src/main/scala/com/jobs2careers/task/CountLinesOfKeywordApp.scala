package com.jobs2careers.task

import com.jobs2careers.base.SparkBaseApp
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by carl on 6/9/15.
 */

class CountLinesOfKeywordApp extends SparkBaseApp{

  val log = LoggerFactory.getLogger("CountLinesOfKeywordApp")

  override def getAppName():String = {
    "CountLinesOfKeywordApp"
  }

  override def executeTask(params : Array[String]): Unit ={
    val config = ConfigFactory.load()
    val conf = getSparkConf(config)
    val sc = new SparkContext(conf)

    val logFile = "file:///" + config.getString("spark.context.home") + "/README.md"
    val keyword = "a"

    log.info("Prepare the resource from %s".format(logFile))
    val rdd = generateRdd(sc, logFile)
    log.info("Executing the calculation based on keyword %s".format(keyword))
    val result = processRows(rdd, keyword)
    log.info("Lines with keyword %s : %s".format(keyword, result))

    sc.stop
  }

  def generateRdd(sc:SparkContext, logFile:String) : RDD[String] = {
    val logData = sc.textFile(logFile, 10)
    logData
  }

  def processRows(rows: RDD[String], keyword: String):Long = {
    val numA = rows.filter(line => line.contains(keyword)).count()
    numA
  }
}
