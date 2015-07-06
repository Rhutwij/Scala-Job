package com.jobs2careers.apps

import com.jobs2careers.base.{ SparkLocalConfig, RedisConfig }
import com.jobs2careers.utilities.ClassPathResourceLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfter, FunSpec }
import com.redis._
import play.api.libs.json.{ JsValue, Json }
import com.jobs2careers.utilities.SharedSparkContext

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
class RedisExportIntegrate extends FunSpec with BeforeAndAfter with SharedSparkContext with RedisConfig {
  private val fixture = "fixtures/sample_mail_update.log"
  private var sqlContext: SQLContext = _
  private var mailUpdateDataFrame: DataFrame = _

  before {
    sqlContext = new SQLContext(sc)
    val resultsBanner =
      """
        |********************************
        |*            Results           *
        |********************************
      """.stripMargin
    val resource = new ClassPathResourceLoader()
      .loadResource(fixture)
    assert(resource != None, s"Test fixture $fixture does not exist!")
    val fixtureFile = resource.get
    val fixturesPath = fixtureFile.getPath
    mailUpdateDataFrame = sqlContext.jsonFile(fixturesPath)
  }

  describe("Integrate UserProfiles to Redis Database") {
    it("should return the correct number of profiles") {
      val profiles: RDD[UserProfile] = UserProfileJob.transform(mailUpdateDataFrame)

      profiles.count() should be(23)
    }
    it("should integrate with Redis") {
      val profiles: RDD[UserProfile] = UserProfileJob.transform(mailUpdateDataFrame)
      UserProfileJob.transport(profiles)
      val redis = new RedisClient(BIG_DATA_REDIS_DB_HOST, BIG_DATA_REDIS_DB_PORT)
      val jobslist: Option[String] = redis.get("joetorres0859@gmail.com")
      val jobs = Json.parse(jobslist.get).as[Seq[String]]
      jobs should contain("1841516234")
      jobs should contain("1785553689")
      //      println(jobslist)
      //      jobslist should be ("abcdefg")

    }
  }
}
