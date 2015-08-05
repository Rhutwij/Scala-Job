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
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

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
class UserProfileSpec extends FunSpec with BeforeAndAfter with SharedSparkContext with RedisConfig {
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

  describe("UserProfileApp") {
    it("should return the correct number of profiles") {
      val profiles: RDD[UserProfile] = UserProfileJob.transform(mailUpdateDataFrame)

      profiles.count() should be(22)
    }
   // it("should serialize to JSON") {
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

//      val expected = """{"userId":"wenjing@jobs2careers.com","mailImpressions":[{"sent":"2015-07-22T16:34:41.000Z","jobs":[1,2,3]},{"sent":"2015-07-21T16:34:41.000Z","jobs":[4,5,6]}]}"""
//
//      val impression1 = MailImpressions("2015-07-22T16:34:41.000Z", Seq(1, 2, 3))
//      val impression2 = MailImpressions("2015-07-21T16:34:41.000Z", Seq(4, 5, 6))
//      val profile = UserProfile("wenjing@jobs2careers.com", Seq(impression1, impression2))
//
//      val actual = UserProfileJob.serialize(profile)
//      expected should be(actual)
//    }
//    it("should give me the output in JSON format") {
//      val profiles: RDD[UserProfile] = UserProfileJob.transform(mailUpdateDataFrame)
//      profiles.foreach { println }
//    }
  }
}
