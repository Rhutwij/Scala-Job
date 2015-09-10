package com.jobs2careers.apps

import org.apache.spark.sql.SQLContext
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.scalatest.BeforeAndAfter
import org.scalatest.Finders
import org.scalatest.FunSpec
import org.scalatest.Matchers.be
import org.scalatest.Matchers.convertToAnyShouldWrapper
import com.jobs2careers.utilities.ClassPathResourceLoader
import com.jobs2careers.utilities.SharedSparkContext
import org.apache.spark.sql.DataFrame
import java.io.ByteArrayOutputStream
import java.io.PrintStream

class DataRegistrySpec extends FunSpec with BeforeAndAfter with SharedSparkContext {
  private var sqlContext: SQLContext = _
  private val cpLoader = new ClassPathResourceLoader()
  val fixturesPath = cpLoader.loadResource("fixtures/").get.getPath

  before {
    sqlContext = new SQLContext(sc)
  }

  it("be able to be run twice") {
    val expected = Seq("src/test/resources/fixtures/2015/07/07/*.log", "src/test/resources/fixtures/2015/07/06/*.log", "src/test/resources/fixtures/2015/07/05/*.log")

    val mailImpressionPaths = DataRegistry.datePaths(3, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 7))
    mailImpressionPaths should be(expected)

    val mailImpressionPaths2 = DataRegistry.datePaths(3, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 7))
    mailImpressionPaths2 should be(expected)
  }

  it("grab today's date as well") {
    val expected = Seq("src/test/resources/fixtures/2015/07/08/*.log", "src/test/resources/fixtures/2015/07/07/*.log", "src/test/resources/fixtures/2015/07/06/*.log")

    val mailImpressionPaths = DataRegistry.datePaths(3, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 8))
    mailImpressionPaths should be(expected)
  }

  it("be able to be classpath patchs") {
    val expected = Seq(s"$fixturesPath/2015/07/07/*.log", s"$fixturesPath/2015/07/06/*.log", s"$fixturesPath/2015/07/05/*.log")

    val mailImpressionPaths = DataRegistry.datePaths(3, fixturesPath + "/", "*.log", new LocalDate(2015, 7, 7))
    mailImpressionPaths should be(expected)

    val mailImpressionPaths2 = DataRegistry.datePaths(3, fixturesPath + "/", "*.log", new LocalDate(2015, 7, 7))
    mailImpressionPaths2 should be(expected)
  }

  it("be able to load paths") {

    val input = Seq(s"$fixturesPath/2015/07/07/*.log", s"$fixturesPath/2015/07/06/*.log", s"$fixturesPath/2015/07/05/*.log")

    val df: DataFrame = DataRegistry.load(sqlContext,sc, input)

    df.count should be(88)
  }

  it("load null timestamps values") {

    val input = Seq(s"$fixturesPath/2015/07/07/*.log", s"$fixturesPath/2015/07/06/*.log", s"$fixturesPath/2015/07/05/*.log", s"$fixturesPath/2015/07/15/*.log")

    val df: DataFrame = DataRegistry.load(sqlContext,sc, input)

    df.count should be(99)
  }

  it("be able to load paths even if they don't exist") {

    val input = Seq(s"$fixturesPath/2015/07/04/*.log", s"$fixturesPath/2015/07/07/*.log", s"$fixturesPath/2015/07/06/*.log", s"$fixturesPath/2015/07/05/*.log")
    val capturedOut = new ByteArrayOutputStream
    val printStream = new PrintStream(capturedOut)
    Console.withOut(printStream) {
      Console.withErr(printStream) {
        val df: DataFrame = DataRegistry.load(sqlContext,sc, input)
        df.count should be(88)
        println(capturedOut)
        capturedOut.toString().length() should be >= 100
      }
    }
    capturedOut.close()
  }

  it("read multiple files from previous 2 weeks") {
    val mailImpressionPaths: Seq[String] = DataRegistry.datePaths(3, s"$fixturesPath/", "sample_mail_update.log", new LocalDate(2015, 7, 7))
    val pubMailImpressionPaths: Seq[String] = DataRegistry.datePaths(3, s"$fixturesPath/", "sample_pubmail_update.log", new LocalDate(2015, 7, 7))
//    mailImpressionPaths.foreach { println }
    if (mailImpressionPaths.isEmpty ||pubMailImpressionPaths.isEmpty ) {
      fail("Files do not exist, cannot read files")
    } else {
      val MailDataFrame = DataRegistry.load(sqlContext,sc, mailImpressionPaths)
      val pubMailDataFrame = DataRegistry.load(sqlContext,sc, pubMailImpressionPaths)
      val profiles = UserProfileFunctionLib.transform(MailDataFrame,pubMailDataFrame)
      profiles.count() should be(32)
    }
  }
}

  // def alwaysTrueFilter(s3Path: String) = true
  //
  //  describe("read multiple files from the previous 2 weeks") {
  //    it("The total number of files should equal to 23") {
  //      val mailImpressionPaths = DataRegistry.datePaths(14, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 8), alwaysTrueFilter)
  //      val MailDataFrame = sqlContext.jsonFile(mailImpressionPaths)
  //      val profiles = UserProfileJob.transform(MailDataFrame)
  //      profiles.count() should be(23)
  //    }
  //    it("three paths should be returned") {
  //      val expected = "src/test/resources/fixtures/2015/07/07/*.log,src/test/resources/fixtures/2015/07/06/*.log,src/test/resources/fixtures/2015/07/05/*.log"
  //
  //      val mailImpressionPaths = DataRegistry.datePaths(3, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 8), alwaysTrueFilter)
  //      mailImpressionPaths should be(expected)
  //    }
  //
  //    it("be able to be run twice") {
  //      val expected = "src/test/resources/fixtures/2015/07/07/*.log,src/test/resources/fixtures/2015/07/06/*.log,src/test/resources/fixtures/2015/07/05/*.log"
  //
  //      val mailImpressionPaths = DataRegistry.datePaths(3, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 8), alwaysTrueFilter)
  //      mailImpressionPaths should be(expected)
  //
  //      val mailImpressionPaths2 = DataRegistry.datePaths(3, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 8), alwaysTrueFilter)
  //      mailImpressionPaths2 should be(expected)
  //    }
  //    
  //    it ("load bad patchs") {
  //          val goodDataFrame = DataRegistry.loadDataFrames(sqlContext,
  //              Seq(fixturePathToFullPath("fixture/2015/07/05/"), fixturePathToFullPath("fixture/2015/07/06/"),
  //                  fixturePathToFullPath("fixture/2015/07/07/")))
  //          val expectedSize = goodDataFrame.count
  //          
  //          val badDataFrame = DataRegistry.loadDataFrames(sqlContext, Seq(fixturePathToFullPath("fixture/2015/07/04/"),
  //              fixturePathToFullPath("fixture/2015/07/05/"), fixturePathToFullPath("fixture/2015/07/06/"),
  //                  fixturePathToFullPath("fixture/2015/07/07/")))
  //          
  //          badDataFrame.count should be(expectedSize)      
  //    }
  //  }
  // 
  //  def fixturePathToFullPath(fixturePath: String): String ={
  //    val resourceFile = cpLoader.loadResource(fixturePath)
  //    fileToPath(resourceFile)
  //  }
  //  
  //  def fileToPath(file: Option[File]): String = {
  //    file match {
  //      case Some(f) => f.getPath + "*.log"
  //      case None => ""
  //    }  
  //   
  //  }
