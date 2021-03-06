package com.jobs2careers.apps

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

import com.jobs2careers.utils.LogLike

object DataRegistry extends LogLike {
  val fmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd/")

  def mail(sqlContext: SQLContext, sc: SparkContext, previousDays: Int, dateEnd: LocalDate = new LocalDate): DataFrame = {
    val logPaths = datePaths(previousDays, "s3n://jiantest/mail/", "*/*.bz2", dateEnd)
    load(sqlContext, sc, logPaths)
  }

  def pubMail(sqlContext: SQLContext, sc: SparkContext, previousDays: Int, dateEnd: LocalDate = new LocalDate): DataFrame = {
    val logPaths = datePaths(previousDays, "s3n://jiantest/api/", "*/*.bz2", dateEnd)
    load(sqlContext, sc, logPaths)
  }

  def datePaths(days: Integer, prefix: String, suffix: String,
                datetime: LocalDate = new LocalDate()): Seq[String] =
    {
      val previousDays = 0 to (days - 1)
      val previousDatePaths: Seq[String] = previousDays map { a =>
        val prevdate = datetime.minusDays(a)
        val datestring = fmt.print(prevdate)
        val fixturepath = prefix + datestring + suffix
        fixturepath
      }

      previousDatePaths
    }

  def load(sqlContext: SQLContext, sc: SparkContext, paths: Seq[String]): DataFrame =
    {

      val rawImpressionsData: Seq[RDD[String]] = paths.flatMap { path =>
        try {
          val dataFrame: RDD[String] = sc.textFile(path, 5)
          //take one to ensure it exists
          dataFrame.take(1)
          Some(dataFrame)
        } catch {
          case e: Throwable =>
            logger.warn(s"Unable to load $path", e)
            None
        }
      }
      val unionFlatFile: RDD[String] = rawImpressionsData.reduce { (a, b) =>
        a.union(b)
      }

      val unionedDataFrame: DataFrame = sqlContext.read.json(unionFlatFile)
      //check if data is corrupt
      val corruptRecord: Boolean = unionedDataFrame.schema.fieldNames.contains("_corrupt_record")

      //countermeasure for corrupt data..if data is corrupt we only get non corrupt data and still read the file
      if (corruptRecord) {
        logger.info("Corrupt records in file path" + paths.mkString(","))
        unionedDataFrame.where(unionedDataFrame("_corrupt_record").isNull)
      } else {
        unionedDataFrame
      }
    }

}
