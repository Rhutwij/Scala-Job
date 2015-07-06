package com.jobs2careers.apps

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ GroupedData, DataFrame }
import com.jobs2careers.base.RedisConfig
import com.redis._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import org.apache.spark.sql.types.StructType

/**
 * Created by wenjing on 7/1/15.
 */
object UserProfileJob extends RedisConfig {

  def transform(mailUpdateDataFrame: DataFrame): RDD[UserProfile] = {
    import mailUpdateDataFrame.sqlContext.implicits._

    val emailToImpressionsDf: DataFrame = mailUpdateDataFrame.select(
      mailUpdateDataFrame("email"), mailUpdateDataFrame("impressions.id"))

    //in 1.4, this will be available off of row
    val fieldNames: Map[String, Int] = emailToImpressionsDf.schema.fieldNames.zipWithIndex.toMap

    val userProfiles: RDD[(String, Seq[String])] = emailToImpressionsDf map { row =>
      // in 1.4, we can do the following
      // val email = row.getAs[String]("email")
      // val impressions = row.getAs[Seq[String]]("id")
      val email = row.getAs[String](fieldNames("email"))
      val impressions = row.getAs[Seq[String]](fieldNames("id"))
      (email, impressions)
    }

    val emailToUserProfiles = userProfiles.reduceByKey { (a, b) => a ++ b }
    emailToUserProfiles map { case (email, jobIds) => UserProfile(email, jobIds) }
  }

  def transport(userProfiles: RDD[UserProfile]): Unit =
    {
      userProfiles.foreachPartition { partition =>
        val redis = new RedisClient(BIG_DATA_REDIS_DB_HOST, BIG_DATA_REDIS_DB_PORT)
        partition.foreach {
          case (UserProfile(email, jobIds)) =>
            val jsonval = Json.toJson(jobIds)
            val jsonstr = Json.stringify(jsonval)
            redis.set(email, jsonstr)
        }
        redis.quit
      }
    }
}

case class UserProfile(email: String, jobIds: Seq[String])