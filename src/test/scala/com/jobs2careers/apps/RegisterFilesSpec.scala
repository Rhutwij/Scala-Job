package com.jobs2careers.apps

import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.joda.time.LocalDate
import org.scalatest.BeforeAndAfter
import org.scalatest.Finders
import org.scalatest.FunSpec
import org.scalatest.Matchers.be
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.Matchers.convertToStringShouldWrapper

import com.jobs2careers.utilities.SharedSparkContext

class RegisterFilesSpec extends FunSpec with BeforeAndAfter with SharedSparkContext {
  private var sqlContext: SQLContext = _
  val cal = Calendar.getInstance

  before {
    cal.set(2015, 7, 7)
    sqlContext = new SQLContext(sc)
  }

  describe("read multiple files from the previous 2 weeks") {
    it("The total number of files should equal to 23") {
      val mailImpressionPaths = DataRegistry.datePaths(3, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 8))
      val MailDataFrame = sqlContext.jsonFile(mailImpressionPaths)
      val profiles = UserProfileJob.transform(MailDataFrame)
      profiles.count() should be(23)
    }
    it("three paths should be returned") {
      val expected = "src/test/resources/fixtures/2015/07/07/*.log,src/test/resources/fixtures/2015/07/06/*.log,src/test/resources/fixtures/2015/07/05/*.log"

      val mailImpressionPaths = DataRegistry.datePaths(3, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 8))
      mailImpressionPaths should be(expected)
    }

    it("be able to be run twice") {
      val expected = "src/test/resources/fixtures/2015/07/07/*.log,src/test/resources/fixtures/2015/07/06/*.log,src/test/resources/fixtures/2015/07/05/*.log"

      val mailImpressionPaths = DataRegistry.datePaths(3, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 8))
      mailImpressionPaths should be(expected)

      val mailImpressionPaths2 = DataRegistry.datePaths(3, "src/test/resources/fixtures/", "*.log", new LocalDate(2015, 7, 8))
      mailImpressionPaths2 should be(expected)
    }
  }

}