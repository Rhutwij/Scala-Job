package com.jobs2careers.apps

import com.jobs2careers.utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import com.jobs2careers.models._
import com.jobs2careers.base.RunSparkJob

/**
 * Created by wenjing on 7/1/15.
 */

object UserProfileJob extends RunSparkJob {

  def main(args: Array[String]) {
    val sc: SparkContext = getSparkContext;
    executeJob(sc, args)

  }

  def executeJob(sc: SparkContext, args: Array[String]): Unit = {

    val sqlContext: SQLContext = new SQLContext(sc)

    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

    //Input
    val numDays: Int = if (args.length > 0) args(0).toInt else 15
    val dateEnd: LocalDate= if (args.length > 0) dateFormat.parseLocalDate(args(1)) else new LocalDate

    //Load the Raw Data
    val userProfiledStart: LocalDateTime = new LocalDateTime
    logger.info(s"The user profiles generate started at $userProfiledStart")
    logger.info(s"Grabbing logs starting at $dateEnd and $numDays before")
    val email_logs: DataFrame = DataRegistry.mail(sqlContext, sc, numDays, dateEnd)
    val pub_logs: DataFrame = DataRegistry.pubMail(sqlContext, sc, numDays, dateEnd)
    
    //getting data and unioning dataframes email and pubemail
    //val emailDataFrame:DataFrame=FunctionLib.getMailDataFrame(email_logs)
    //val pubEmailDataFrame:DataFrame=FunctionLib.getPubMailDataFrame(pub_logs)
    //val unionedMailDataFrame:DataFrame=FunctionLib.unionMailDataFrames(emailDataFrame, pubEmailDataFrame)

    // Create profile RDD from raw data dataframe
    val profiles: RDD[UserProfile] = UserProfileFunctionLib.transform(email_logs,pub_logs)

    //Upload data to redis
    UserProfileFunctionLib.transport(profiles)

    //Check number of profiles (debug)
    val userProfiledFinished: LocalDateTime = new LocalDateTime
    logger.info(s"Created ${profiles.count()} profiles!")
    logger.info(s"The user profile generation was started at $userProfiledStart and finished at $userProfiledFinished")
    //StopingJob
    sc.stop()
  }
}
