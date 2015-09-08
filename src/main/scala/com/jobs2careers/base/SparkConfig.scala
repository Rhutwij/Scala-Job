package com.jobs2careers.base
import com.typesafe.config.ConfigFactory
import org.apache.spark.{ SparkConf, SparkContext }
/**
 * @author rtulankar
 */
trait SparkConfig {

  private val config = ConfigFactory.load()
  val sparkConf = new SparkConf()
  sparkConf.setAppName(this.getClass.getName)
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
}