package com.jobs2careers.apps

import com.jobs2careers.base.{ SparkLocalConfig, RedisConfig }
import com.jobs2careers.utilities.ClassPathResourceLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfter, FunSpec }
import redis.RedisClient
import akka.actor.ActorSystem
import akka.util.ByteString
import redis.{ RedisClientMasterSlaves, RedisServer }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.{ Await, Future }
import play.api.libs.json.{ JsValue, Json }
import com.jobs2careers.utilities.SharedSparkContext
import com.jobs2careers.utils._
import com.jobs2careers.models._

/**
 * Example ScalaTest
 *
 * Unit tests are typically named $CLASS_NAME+Spec
 * Integration tests are typically named $CLASS_NAME+ForIntegrationSpec
 *
 * Typically describe(){} blocks are used to organize the class and method
 * level while it(){} blocks are used for each method behavior test.
 *
 * If you use a SLF4J logger in your classes, don't expect to see the logs here.
 * For some reason ScalaTest swallows all the logs.
 *
 * You can run tests by right clicking on the class, or any of the it or
 * describe blocks.
 */
class RedisExportIntegrate extends FunSpec with BeforeAndAfter with SharedSparkContext with RedisConfig with LogLike {
  private val emailFixture = "fixtures/sample_mail_update2.log"
  private val pubEmailFixture="fixtures/sample_pubmail_update.log"
  private var sqlContext: SQLContext = _
  private var mailUpdateDataFrame: DataFrame = _
  private var pubMailUpdateDataFrame: DataFrame = _
  implicit val akkaSystem = akka.actor.ActorSystem()

  before {
    sqlContext = new SQLContext(sc)
    val resultsBanner =
      """
        |********************************
        |*            Results           *
        |********************************
      """.stripMargin
    val resource = new ClassPathResourceLoader()
      .loadResource(emailFixture)
    val resource2 = new ClassPathResourceLoader()
      .loadResource(pubEmailFixture)
      
    println(resource2.get);
    assert(resource != None, s"Test fixture $emailFixture does not exist!")
    assert(resource2 != None, s"Test fixture $pubEmailFixture does not exist!")
    val fixtureFile = resource.get
    val fixturesPath = fixtureFile.getPath
    val fixtureFile2 = resource2.get
    val fixturesPath2 = fixtureFile2.getPath
    mailUpdateDataFrame = sqlContext.jsonFile(fixturesPath)
    pubMailUpdateDataFrame = sqlContext.jsonFile(fixturesPath2)
  }
  def getValue(key: String, redisclient: RedisClient): Option[String] = {
    val future = redisclient.get(key)
    future onFailure {
      case e => {
        logger.error("Fetch to fetch the value from ElastiCache: " + e.getMessage)
      }
    }
    Await.result(future, 2 seconds) match {
      case Some(v: ByteString) => {
        logger.debug("Receiving the result from remote: " + new String(v.toArray))
        Some(new String(v.toArray))
      }
      case _ => {
        logger.error("Get Wrong format from ElastiCache.")
        None
      }
    }
  }

  describe("Integrate UserProfiles to Redis Database") {
    it("should integrate with Redis") {
      val testUser = "joetorres0859@jobs2careers.com"
      val redis = RedisClient(BIG_DATA_REDIS_DB_HOST, BIG_DATA_REDIS_DB_PORT)

      //clean up from previous run
      redis.del(testUser)

      val profiles: RDD[UserProfile] = FunctionLib.transform(mailUpdateDataFrame,pubMailUpdateDataFrame)
      FunctionLib.transport(profiles)

      val userProfileJson: Option[String] = getValue(testUser, redis)
      //      val jobs = Json.parse(jobslist.get).as[Seq[String]]
      userProfileJson.get should include("1839849788")
      userProfileJson.get should include("4123")
      //      println(jobslist)
      //      jobslist should be ("abcdefg")

    }

    it("should set even if value exists") {
      val testUser = "joetorres0859@jobs2careers.com"
      val redis = new RedisClient(BIG_DATA_REDIS_DB_HOST, BIG_DATA_REDIS_DB_PORT)

      //clean up from previous run
      redis.set(testUser, "")

      val profiles: RDD[UserProfile] = FunctionLib.transform(mailUpdateDataFrame,pubMailUpdateDataFrame)
      FunctionLib.transport(profiles)

      val userProfileJson: Option[String] = getValue(testUser, redis)
      //      val jobs = Json.parse(jobslist.get).as[Seq[String]]
      userProfileJson.get should include("1839849788")
      userProfileJson.get should include("4123")
      //      println(jobslist)
      //      jobslist should be ("abcdefg")

    }
  }
}
