package com.jobs2careers.base
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.{SparkContext, SparkConf}
import com.jobs2careers.utils.LogLike
/**
 * @author rtulankar
 */
trait RunSparkJob extends SparkConfig with LogLike{
  def getSparkContext: SparkContext =  new SparkContext(sparkConf)
  
  val resultsBanner =
    """
      |********************************
      |*            Results           *
      |********************************
    """.stripMargin
   def executeJob(sc:SparkContext,args:Array[String]):Unit
}