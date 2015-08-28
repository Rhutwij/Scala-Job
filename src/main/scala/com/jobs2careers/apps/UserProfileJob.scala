package com.jobs2careers.apps

import scala.collection._
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.joda.time._
import org.joda.time.format.DateTimeFormat

import com.jobs2careers.apps._
import com.jobs2careers.base.RedisConfig
import com.jobs2careers.utils._

import akka.actor.ActorSystem
import play.api.libs.json._
import redis.RedisClient

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
//TODO use iterable instead of sequence
//case class UserProfile(userId: String, mailImpressions: Iterable[MailImpressions])
case class UserProfile(userId: String, mailImpressions: Seq[MailImpressions])

/**
 * Created by wenjing on 7/1/15.
 */

object UserProfileJob extends RedisConfig with LogLike {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Mail User Recommendation Profiles")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(conf);
    val sqlContext: SQLContext = new SQLContext(sc)

    val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

    //Input
    val numDays = if (args.length > 0) args(0).toInt else 15
    val dateEnd = if (args.length > 0) dateFormat.parseLocalDate(args(1)) else new LocalDate

    //Load the Raw Data
    val userProfiledStart = new LocalDateTime
    println(s"The user profiles generate started at $userProfiledStart")
    logger.info(s"Grabbing logs starting at $dateEnd and $numDays before")
    val email_logs = DataRegistry.mail(sqlContext, numDays, dateEnd)

    // Create profile RDD from raw data dataframe
    val profiles: RDD[UserProfile] = UserProfileJob.transform(email_logs)

    //Upload data to redis
    UserProfileJob.transport(profiles)

    //Check number of profiles (debug)
    val userProfiledFinished = new LocalDateTime
    println(s"Created ${profiles.count()} profiles!")
    println(s"The user profile generation was started at $userProfiledStart and finished at $userProfiledFinished")

    sc.stop()
  }
  implicit val akkaSystem = akka.actor.ActorSystem()
  implicit val mailImpressionsFormat = Json.format[MailImpressions]
  implicit val profileFormat = Json.format[UserProfile]

  def transform(mailUpdateDataFrame: DataFrame): RDD[UserProfile] = {

    // select the useful information
    val emailToImpressionsDf: DataFrame = mailUpdateDataFrame.select(
      mailUpdateDataFrame("email"), mailUpdateDataFrame("impressions.id"),
      mailUpdateDataFrame("timestamp")).where(mailUpdateDataFrame("timestamp").isNotNull).where(mailUpdateDataFrame("timestamp").notEqual("null"))

    /*
    val userProfiles: RDD[(String, UserProfile)] = emailToImpressionsDf map { row =>
      // in 1.4, we can do the following
      // val email = row.getAs[String]("email")
      // val impressions = row.getAs[Seq[String]]("id")
      val email = row.getAs[String]("email")
      val impressions: Seq[String] = row.getAs[Seq[String]]("id").filter(e => e != "null" )
      val longImpressions: Seq[Long] = impressions.map { _.toLong }
      val sent = row.getAs[String]("timestamp").substring(0,10)
      (email, UserProfile(email, Seq(MailImpressions(sent, longImpressions))))
    }
*/
    /*
    val userIdToUserProfilesCombined = userProfiles.reduceByKey { (p1, p2) =>
      p1.copy(mailImpressions = p1.mailImpressions ++ p2.mailImpressions)
    }
    */

    val userProfiles: RDD[(String, Seq[MailImpressions])] = emailToImpressionsDf map { row =>
      val email = row.getAs[String]("email")
      val impressions: Seq[String] = row.getAs[Seq[String]]("id").filter(e => e != "null")
      val longImpressions: Seq[Long] = impressions.map {
        _.toLong
      }
      val sent = row.getAs[String]("timestamp").substring(0, 10)
      (email, Seq(MailImpressions(sent, longImpressions)))
    }

    val userIdToUserProfilesCombined: RDD[(String, Seq[MailImpressions])] =
      userProfiles.reduceByKey((q1, q2) => q1 ++ q2)

    // Impressions is categorized on user level.
    // Now remove redundant sent time
    val userIdToUserProfilesMerged: RDD[UserProfile] = userIdToUserProfilesCombined.map {
      case (email: String, impressions: Seq[MailImpressions]) =>
        val sentImpressions: Map[String, Seq[MailImpressions]] = impressions.groupBy(impressions => impressions.sent)
        val combinedSentImpressions = sentImpressions mapValues { listMailImpressions =>
          listMailImpressions reduce { (a, b) =>
            a.copy(jobs = a.jobs ++ b.jobs)
          }
        }
        val userImpressions = combinedSentImpressions.values.toList
        UserProfile(email, userImpressions)
    }
    userIdToUserProfilesMerged
  }

  def transport(userProfiles: RDD[UserProfile]): Long = {
    // 6 days * 24 hours / day * 60 minutes / hour * 60 seconds / minute 
    val profileExpiration = 6 * 24 * 60 * 60
    val userProfilesMapRDDs: RDD[Boolean] = userProfiles.mapPartitions { partition: Iterator[UserProfile] =>
      val redis: RedisClient = RedisClient(BIG_DATA_REDIS_DB_HOST, BIG_DATA_REDIS_DB_PORT)
      try {
        val redisFutureSets: Iterator[Future[Boolean]] = partition.map {
          case (profile: UserProfile) =>
            val userId: String = profile.userId
            val jsonstr: String = serialize(profile)
            val returnedfuture: Future[Boolean] = PutProfileRedis(userId, profileExpiration, redis, jsonstr);
            returnedfuture
        }
        val listOfFutures: List[Future[Boolean]] = redisFutureSets.toList;
        val finalFutureList: Future[List[Boolean]] = Future.sequence(listOfFutures)
        val successes: List[Boolean] = Await.result(finalFutureList, 1 hours)
        val numSuccesses: List[Boolean] = successes.map { s => s.booleanValue() }.filter { value => value.==(true) }
        numSuccesses.toIterator
      } finally {
        redis.quit
      }
    }
    val totalUserProfilesSetSuccessfully: Long = userProfilesMapRDDs.count()
    val totalUserProfiles: Long = userProfiles.count()
    logger.info("Total UserProfiles:" + totalUserProfiles + " Actual Futures Set" + totalUserProfilesSetSuccessfully)
    totalUserProfiles
  }

  def PutProfileRedis(key: String, exp: Int, redisClient: RedisClient, value: String): Future[Boolean] = {
    //kick it three times max

    val future: Future[Boolean] = redisClient.setex(key, exp, value) recover {
      case _ =>
        val attempt2: Future[Boolean] = redisClient.setex(key, exp, value) recover {
          case _ =>
            val attempt3: Future[Boolean] = redisClient.setex(key, exp, value)
            Await.result(attempt3, 6 seconds)
        }

        Await.result(attempt2, 10 seconds)
    }

    future
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
        logger.error(s"Unable to deserialize user profile $json", e)
        None
      }
    }
  }
}
