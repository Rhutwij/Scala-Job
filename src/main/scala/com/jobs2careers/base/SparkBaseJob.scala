package com.jobs2careers.base

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * This trait exposes a Spark Context and the Spark Configuration to anyone
 * mixing it in.
 */
trait SparkBaseJob extends Serializable with SparkLocalConfig {
  def getSparkContext: SparkContext =  new SparkContext(sparkConf)
  val resultsBanner =
    """
      |********************************
      |*            Results           *
      |********************************
    """.stripMargin
}
