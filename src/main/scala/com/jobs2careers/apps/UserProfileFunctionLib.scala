package com.jobs2careers.apps

/**
 * @author rtulankar
 */
import com.jobs2careers.base.RedisConfig
import com.jobs2careers.utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import play.api.libs.json._
import redis.RedisClient
import com.jobs2careers.models._
import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import org.apache.commons.codec.digest.DigestUtils

object UserProfileFunctionLib extends LogLike with RedisConfig with HashFunctions {

  implicit val akkaSystem = akka.actor.ActorSystem()
  implicit val mailImpressionsFormat = Json.format[MailImpressions]
  implicit val profileFormat = Json.format[UserProfile]
  val impressionLimit = applicationConfiguration.getInt("impression.limit");

  def getMailDataFrame(mailDataFrame: DataFrame): DataFrame = {
    val emailToImpressionsDf: DataFrame = mailDataFrame.select(
      mailDataFrame("email"), mailDataFrame("impressions.id").alias("id"),
      mailDataFrame("timestamp")).where(mailDataFrame("timestamp").isNotNull).where(mailDataFrame("timestamp").notEqual("null"))
    emailToImpressionsDf
  }

  def getPubMailDataFrame(pubMailDataFrame: DataFrame): DataFrame = {
    val pubEmailToImpressionsDf: DataFrame = pubMailDataFrame.select(
      pubMailDataFrame("eid").as("email"), pubMailDataFrame("jobID").as("id"),
      pubMailDataFrame("timestamp")).where(pubMailDataFrame("timestamp").isNotNull)
      .where(pubMailDataFrame("timestamp").notEqual("null"))

    val pubEmailToImpressionsDfMappedToEmail = pubEmailToImpressionsDf.filter(pubEmailToImpressionsDf("email") !== "")

    pubEmailToImpressionsDfMappedToEmail
  }

  def transformImpressions(impressionDataFrame: DataFrame): RDD[(String, Seq[MailImpressions])] = {
    val impressionsTransformedRDD: RDD[(String, Seq[MailImpressions])] = impressionDataFrame map { row =>
      val email = row.getAs[String]("email")
      val impressions: Seq[String] = row.getAs[Seq[String]]("id").filter(e => e != "null")
      val longImpressions: Seq[Long] = impressions.map {
        _.toLong
      } 
      val topFiveImpressions: Seq[Long] = longImpressions.slice(0, 5)
      val sent = row.getAs[String]("timestamp").substring(0, 10)
      (email, Seq(MailImpressions(sent, topFiveImpressions)))
    }
    impressionsTransformedRDD
  }
  
  def impressionReduction(impressions: RDD[(String, Seq[MailImpressions])]): RDD[(String, Seq[MailImpressions])]={
    val reductionResult:RDD[(String, Seq[MailImpressions])]=impressions.reduceByKey((q1, q2) => q1 ++ q2)
    reductionResult
  }
  
  
  def unionImpressionsRDD(rdd1:RDD[(String, Seq[MailImpressions])],rdd2:RDD[(String, Seq[MailImpressions])]):RDD[(String, Seq[MailImpressions])]={
    rdd1.union(rdd2)
  }
  

  def groupImpressionsBySentTime(impressions: RDD[(String, Seq[MailImpressions])]): RDD[UserProfile]={
    impressions.map {
      case (email: String, impressions1: Seq[MailImpressions]) =>
        val sentImpressions: Map[String, Seq[MailImpressions]] = impressions1.groupBy(impressions1 => impressions1.sent)
        val combinedSentImpressions: Map[String, MailImpressions] = sentImpressions mapValues { listMailImpressions =>
          listMailImpressions reduce { (a, b) =>
            a.copy(jobs = a.jobs ++ b.jobs)
          }
        }
        //limiting number of impressions per day of same user
       val reduceImpressions:Map[String,MailImpressions]=combinedSentImpressions.map{
         case (eid:String,imp:MailImpressions)=> 
         
           (eid.toString(),imp.copy(imp.sent,imp.jobs.slice(0, impressionLimit)))
         }
       
       val userImpressions = reduceImpressions.values.toList;
        
        UserProfile(email, userImpressions)
    }
    
  }
  
  def transform(mailUpdateDataFrame: DataFrame, pubMailUpdateDataFrame: DataFrame): RDD[UserProfile] = {

    // select the useful information
    val emailToImpressionsDf: DataFrame = getMailDataFrame(mailUpdateDataFrame)

    val pubEmailToImpressionsDf: DataFrame = getPubMailDataFrame(pubMailUpdateDataFrame)

    val userEmailProfiles: RDD[(String, Seq[MailImpressions])] = transformImpressions(emailToImpressionsDf)

    val userPubEmailProfiles: RDD[(String, Seq[MailImpressions])] = transformImpressions(pubEmailToImpressionsDf)
    val userProfiles: RDD[(String, Seq[MailImpressions])] = unionImpressionsRDD(userEmailProfiles,userPubEmailProfiles)

    val userIdToUserProfilesCombined: RDD[(String, Seq[MailImpressions])] =impressionReduction(userProfiles)

    // Impressions is categorized on user level.
    // Now remove redundant sent time
    val userIdToUserProfilesMerged: RDD[UserProfile] = groupImpressionsBySentTime(userIdToUserProfilesCombined)
    //returning profiles
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