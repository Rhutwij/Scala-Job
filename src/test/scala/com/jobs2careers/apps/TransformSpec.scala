package com.jobs2careers.apps

import com.jobs2careers.base.RedisConfig
import com.jobs2careers.utilities.{ ClassPathResourceLoader, SharedSparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.Matchers._
import org.scalatest.{ BeforeAndAfter, FunSpec }
import com.jobs2careers.models._
import com.jobs2careers.utils.HashFunctions
/**
 * @author rtulankar
 * @About the TransformSpec
 * This class test the transformations on Publisher clicks reading sample data from resources folder
 * 1) Test 1 & 2is getMailDataFrame && getPubMailDataFrame
 * 2) Test 3 & 4 id Testing transformImpressions function on both getPubMailDataFrame  && getPubMailDataFrame
 * 3) Test 5 testing unionImpressionsRDD
 * 4) Test 6 testing impressionReduction
 * 5) Test 7 testing groupImpressionsBySentTime
 *
 */
class TransformSpec extends FunSpec with BeforeAndAfter with SharedSparkContext with RedisConfig with HashFunctions {
  private val emailFixture = "fixtures/sample_mail_update.log"
  private val emailFixture_multiple = "fixtures/multiple_mail_entries.log"
  private val PubEmailFixture = "fixtures/sample_pubmail_update.log"
  private var sqlContext: SQLContext = _
  private var mailUpdateDataFrame: DataFrame = _
  private var pubMailUpdateDataFrame: DataFrame = _
  private var pubMailUpdateDataFrame_multiple: DataFrame = _

  before({
    sqlContext = new SQLContext(sc)
    val resultsBanner =
      """
        |********************************
        |*            Results           *
        |********************************
      """.stripMargin
    val resource = new ClassPathResourceLoader()
      .loadResource(emailFixture)
    assert(resource != None, s"Test fixture $emailFixture does not exist!")
    val fixtureFile = resource.get
    val fixturesPath = fixtureFile.getPath
    val resource2 = new ClassPathResourceLoader()
      .loadResource(PubEmailFixture)
    assert(resource2 != None, s"Test fixture $PubEmailFixture does not exist!")
    val fixtureFile2 = resource2.get
    val fixturesPath2 = fixtureFile2.getPath
    //third data frame
    val resource3 = new ClassPathResourceLoader()
      .loadResource(emailFixture_multiple)
    assert(resource3 != None, s"Test fixture $PubEmailFixture does not exist!")
    val fixtureFile3 = resource3.get
    val fixturesPath3 = fixtureFile3.getPath
    mailUpdateDataFrame = DataRegistry.load(sqlContext, sc, Seq(fixturesPath))
    pubMailUpdateDataFrame = DataRegistry.load(sqlContext, sc, Seq(fixturesPath2))
    pubMailUpdateDataFrame_multiple = DataRegistry.load(sqlContext, sc, Seq(fixturesPath3))
  })

  describe("UserProfileApp") {

    //Test1
    it("should read pubMailUpdateDataFrame and return results total below") {
      val profiles: DataFrame = UserProfileFunctionLib.getPubMailDataFrame(pubMailUpdateDataFrame)

      profiles.count() should be(12)
    }

    //Test2
    it("should read mailUpdateDataFrame and return results total below") {
      val profiles: DataFrame = UserProfileFunctionLib.getMailDataFrame(mailUpdateDataFrame)

      profiles.count() should be(41)
    }

    //Test3
    it("should read transform getPubMailDataFrame impressions in RDD[(String, Seq[MailImpressions])] ") {
      val profiles: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getPubMailDataFrame(pubMailUpdateDataFrame))

      profiles.count() should be(12)
    }

    //Test4
    it("should read transform MailDataFrame impressions in RDD[(String, Seq[MailImpressions])] ") {
      val profiles: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getMailDataFrame(mailUpdateDataFrame))
      profiles.count() should be(41)
    }

    //Test5
    it("unionImpressionsRDD test the total should be equal ") {
      val profiles1: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getMailDataFrame(mailUpdateDataFrame))
      val profiles2: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getPubMailDataFrame(pubMailUpdateDataFrame))
      val unionedProfiles = UserProfileFunctionLib.unionImpressionsRDD(profiles1, profiles2)
      unionedProfiles.count() should be(53)
    }
    //Test5.1
    it("unionImpressionsRDD NULL test the total should be equal ") {
      val profiles1: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getPubMailDataFrame(pubMailUpdateDataFrame))
      val profiles2: RDD[(String, Seq[MailImpressions])] = sc.emptyRDD
      val unionedProfiles = UserProfileFunctionLib.unionImpressionsRDD(profiles1, profiles2)
      unionedProfiles.count() should be(12)
    }
    //Test5.2
    it("getting top5 Impressions") {
      val profiles1: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getMailDataFrame(mailUpdateDataFrame))
      val impressions: Array[(String, Seq[MailImpressions])] = profiles1.take(1);
      val impressionsArray: Array[MailImpressions] = impressions.flatMap { case (email, imp) => imp };
      val impressionsCount = impressionsArray.length.toInt
      impressionsCount should be < 6
    }
    //test 6 impression reduction 
    //Test6.1
    it("Impressions Reduction with ONE NULL RDD") {
      val profiles1: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getPubMailDataFrame(pubMailUpdateDataFrame))
      val profiles2: RDD[(String, Seq[MailImpressions])] = sc.emptyRDD
      val unionedProfiles: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.unionImpressionsRDD(profiles1, profiles2)
      val reducedRDD: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.impressionReduction(unionedProfiles)
      reducedRDD.count() should be(9)
    }

    //Test6.2
    it("Impressions Reduction with NO NULL") {
      val profiles1: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getPubMailDataFrame(pubMailUpdateDataFrame))
      val profiles2: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getMailDataFrame(mailUpdateDataFrame))
      val unionedProfiles: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.unionImpressionsRDD(profiles1, profiles2)
      val reducedRDD: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.impressionReduction(unionedProfiles)
      reducedRDD.count() should be(32)
    }

    //Test 7 grouping by time
    it("Grouping impressions by time test") {
      val profiles1: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getPubMailDataFrame(pubMailUpdateDataFrame))
      val profiles2: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getMailDataFrame(mailUpdateDataFrame))
      val unionedProfiles: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.unionImpressionsRDD(profiles1, profiles2)
      val reducedRDD: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.impressionReduction(unionedProfiles)
      val groupedRDD: RDD[UserProfile] = UserProfileFunctionLib.groupImpressionsBySentTime(reducedRDD)
      val pubprof = groupedRDD.map { case UserProfile(x, y) => x }.filter { email => email == "741+rhutwij@jobs2careers.com" }.count()
      val mailprof = groupedRDD.map { case UserProfile(x, y) => x }.filter { email => email == "ethorson2@yahoo.com" }.count()
      pubprof should be(mailprof)
      groupedRDD.count() should be(32)
    }

    //Test to check number of impressions
    it("test to check user impressions per day should not be greater than 15") {
      val profiles1: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getPubMailDataFrame(pubMailUpdateDataFrame_multiple))
      // profiles1.foreach{
      //println
      //}
      val userIdToUserProfilesCombined: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.impressionReduction(profiles1)
      //userIdToUserProfilesCombined.foreach(println)
      val userIdToUserProfilesMerged: RDD[UserProfile] = UserProfileFunctionLib.groupImpressionsBySentTime(userIdToUserProfilesCombined)
      userIdToUserProfilesMerged.map { x => x.mailImpressions }.map { row =>
        if (row.length == 2)
          (row(0).jobs.length, row(1).jobs.length)
        else
          (row(0).jobs.length, 0)
      }.map { row =>
        if (row._1 > 15 || row._2 > 15) //per day the limit should not be > 15
          ("0", 0) //return false if >15 even one value
        else
          ("1", 1) //return true if <15
      }.reduceByKey((a, b) => a + b).collect()(0)._1.toInt should be(1);
      //userIdToUserProfilesMerged.map{x=>x.mailImpressions}.count() should be (2);

    }

    //Test to check number of impressions
    it("test to check user dates total should be less than 2") {
      val profiles1: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.transformImpressions(UserProfileFunctionLib.getPubMailDataFrame(pubMailUpdateDataFrame_multiple))
      val userIdToUserProfilesCombined: RDD[(String, Seq[MailImpressions])] = UserProfileFunctionLib.impressionReduction(profiles1)
      //userIdToUserProfilesCombined.foreach(println)
      val userIdToUserProfilesMerged: RDD[UserProfile] = UserProfileFunctionLib.groupImpressionsBySentTime(userIdToUserProfilesCombined)
      val perdayCount = userIdToUserProfilesMerged.map { x => ("count", x.mailImpressions.length) }.collect();
      perdayCount(0)._2 should be <= (2);
      perdayCount(1)._2 should be <= (2);

    }
  }
}