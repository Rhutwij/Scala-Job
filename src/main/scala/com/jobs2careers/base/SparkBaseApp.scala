package com.jobs2careers.base

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * This trait exposes a Spark Context and the Spark Configuration to anyone
 * mixing it in.
 */
trait SparkBaseApp extends Serializable {
  private val config = ConfigFactory.load()
  val sparkConf = getSparkConf(config)
  val sc = new SparkContext(sparkConf)

  /**
   * Given a TypeSafe config, return the Spark Config.
   * @param config the config
   * @return the SparkConf
   */
  def getSparkConf(config: Config): SparkConf = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setIfMissing("spark.master", "local[*]")
    conf.setSparkHome(config.getString("spark.context.home"))
    conf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf
  }
}
