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
 */
class CorruptJsonSpec extends FunSpec with BeforeAndAfter with SharedSparkContext with RedisConfig with HashFunctions {
  private val pathS3 = "s3n://AKIAJFCO5KRVPEETKPJQ:8jdSSfGvFGNiXg04OohOgfXRqtejAJUwHHycFkw8@jiantest/jobs/2015/07/09/ctlcntr/2015-07-09000055jobs.bz2"
  private val path="fixtures/IncompleteJson"
  private var sqlContext: SQLContext = _
  private var mailUpdateDataFrame: DataFrame = _
  private var fixturesPath=""
  before({
    sqlContext = new SQLContext(sc)
    val resultsBanner =
      """
        |********************************
        |*            Results           *
        |********************************
      """.stripMargin
     val resource = new ClassPathResourceLoader()
      .loadResource(path)
    assert(resource != None, s"Test fixture $path does not exist!")
    val fixtureFile = resource.get
    fixturesPath = fixtureFile.getPath

  })
  describe("Corrupt Data Spec") {
    //Test1
    it("should read pubMailUpdateDataFrame and return results total below") {
      mailUpdateDataFrame = DataRegistry.load(sqlContext, sc, Seq(pathS3,fixturesPath))
      mailUpdateDataFrame.count().toInt should be >1
    }

  }
}