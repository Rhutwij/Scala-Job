package com.jobs2careers.apps

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime

import com.jobs2careers.base.RedisConfig
import com.redis._

import play.api.libs.json._

import org.apache.spark.SparkContext._

//JSON structure
//{
//    "userId": "wenjing@jobs2careers.com",
//    "mailImpressions": [
//        {
//            "sent": "2015-07-22T16:34:41.000Z",
//            "jobs": [
//                1,
//                2,
//                3
//            ]
//        },
//        {
//            "sent": "2015-07-21T16:34:41.000Z",
//            "jobs": [
//                4,
//                5,
//                6
//            ]
//        }
//    ]
//}

case class MailImpressions(sent: String, jobs: Seq[Long])
case class UserProfile(userId: String, mailImpressions: Seq[MailImpressions])

/**
 * Created by wenjing on 7/1/15.
 */
object UserProfileJob extends RedisConfig {

  implicit val mailImpressionsFormat = Json.format[MailImpressions]
  implicit val profileFormat = Json.format[UserProfile]

  def transform(mailUpdateDataFrame: DataFrame): RDD[UserProfile] = {
    import mailUpdateDataFrame.sqlContext.implicits._

    val emailToImpressionsDf: DataFrame = mailUpdateDataFrame.select(
      mailUpdateDataFrame("email"), mailUpdateDataFrame("impressions.id"), mailUpdateDataFrame("timestamp"))

    //in 1.4, this will be available off of row
    val fieldNames: Map[String, Int] = emailToImpressionsDf.schema.fieldNames.zipWithIndex.toMap

    val userProfiles: RDD[(String, UserProfile)] = emailToImpressionsDf map { row =>
      // in 1.4, we can do the following
      // val email = row.getAs[String]("email")
      // val impressions = row.getAs[Seq[String]]("id")
      val email = row.getAs[String](fieldNames("email"))
      val impressions: Seq[String] = row.getAs[Seq[String]](fieldNames("id"))
      val longImpressions: Seq[Long] = impressions.map { _.toLong }
      val sent = row.getAs[String](fieldNames("timestamp"))
      (email, UserProfile(email, Seq(MailImpressions(sent, longImpressions))))
    }
    val userIdToUserProfilesCombined = userProfiles.reduceByKey { (p1, p2) =>
      p1.copy(mailImpressions = p1.mailImpressions ++ p2.mailImpressions)
    }

    userIdToUserProfilesCombined.map { case (_, userProfile) => userProfile }
  }

  def transport(userProfiles: RDD[UserProfile]): Unit = {
    // 37 days * 24 hours / day * 60 minutes / hour * 60 seconds / minute 
    val profileExpiration = 37 * 24 * 60 * 60
    userProfiles.foreachPartition { partition =>
      val redis = new RedisClient(BIG_DATA_REDIS_DB_HOST, BIG_DATA_REDIS_DB_PORT)
      try {
        partition.foreach {
          case (profile: UserProfile) =>
            val userId = profile.userId
            //            val jsonval = Json.toJson(jobIds)
            val jsonstr = serialize(profile)
            redis.setex(userId, profileExpiration, jsonstr)
        }
      } finally { redis.quit }
    }
  }

  def serialize(profile: UserProfile): String = {
    val json = Json.toJson(profile)
    Json.stringify(json)
  }

  def deserialize(json: String): Option[UserProfile] = {
    val jsonValue = Json.parse(json)
    jsonValue.validate[UserProfile] match {
      case s: JsSuccess[UserProfile] => {
        Some(s.get)
      }
      case e: JsError => {
        println(s"Unabel to deserialize user profile $json because $e")
        None
      }
    }
  }
}
