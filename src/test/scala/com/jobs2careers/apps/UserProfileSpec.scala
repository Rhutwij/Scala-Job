package com.jobs2careers.apps

import com.jobs2careers.base.RedisConfig
import com.jobs2careers.utilities.{ClassPathResourceLoader, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FunSpec}

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

  before({
    sqlContext = new SQLContext(sc)
    val resultsBanner =
      """
        |********************************
        |*            Results           *
        |********************************
      """.stripMargin

    mailUpdateDataFrame = createDataFrame(fixture)
  })

  def createDataFrame(fixturePath: String): DataFrame = {
    val resource = new ClassPathResourceLoader()
      .loadResource(fixturePath)
    assert(resource != None, s"Test fixture $fixture does not exist!")
    val fixtureFile = resource.get
    val fixturesPath = fixtureFile.getPath
    sqlContext.jsonFile(fixturesPath)
  }

  describe("UserProfileApp") {

    it("should return the correct number of profiles") {
      val profiles: RDD[UserProfile] = UserProfileJob.transform(mailUpdateDataFrame)

      profiles.count() should be(23)
    }

    it("should work with null timestamps") {
      val mailUpdateNull = createDataFrame("fixtures/sample_mail_update_null.log")

      val profiles: RDD[UserProfile] = UserProfileJob.transform(mailUpdateNull)

      profiles.count() should be(22)
    }


    it("should work with multiple timestamps") {
      val multipleTimestampsLog: DataFrame = createDataFrame("fixtures/different-times.log")
      val profiles: RDD[UserProfile] = UserProfileJob.transform(multipleTimestampsLog)
      profiles.count() should be(3)

      val lnProfile: RDD[UserProfile] = profiles.filter { profile => profile.userId == "lnathnp@hotmail.com" }
      lnProfile.count should be(1)

      val lnProfileLocal: UserProfile = lnProfile.collect()(0)
      lnProfileLocal.mailImpressions.length should be(2)
    }

    it("should work with null impression") {
      val timestampLogs = createDataFrame("fixtures/null-impression_ids.log")
      val profiles: RDD[UserProfile] = UserProfileJob.transform(timestampLogs)
      profiles.count() should be(3)
    }

    it("It should generate a specific json format records for Wenjing from log files"){
      val wenjingLogs = createDataFrame("fixtures/wenjing-impressions.log")
      val profiles: RDD[UserProfile] = UserProfileJob.transform(wenjingLogs)
      profiles.count() should be (1)

      val jsonprofiles = profiles.map{ row =>
        UserProfileJob.serialize(row)
      }
      jsonprofiles.count() should be (1)
    }

    it("should serialize to JSON") {
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

      val expected = """{"userId":"wenjing@jobs2careers.com","mailImpressions":[{"sent":"2015-07-22T16:34:41.000Z","jobs":[1,2,3]},{"sent":"2015-07-21T16:34:41.000Z","jobs":[4,5,6]}]}"""

      val impression1 = MailImpressions("2015-07-22T16:34:41.000Z", Seq(1, 2, 3))
      val impression2 = MailImpressions("2015-07-21T16:34:41.000Z", Seq(4, 5, 6))
      val profile = UserProfile("wenjing@jobs2careers.com", Seq(impression1, impression2))

      val actual = UserProfileJob.serialize(profile)
      expected should be(actual)
    }
    it("should give me the output in JSON format") {
      val profiles: RDD[UserProfile] = UserProfileJob.transform(mailUpdateDataFrame)
      //      profiles.foreach { println }
    }

  }
}
