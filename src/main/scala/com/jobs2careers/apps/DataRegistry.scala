package com.jobs2careers.apps

import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

object DataRegistry {

  val fmt = DateTimeFormat.forPattern("yyyy/MM/dd/")

  def mail(sqlContext: SQLContext, previousDays: Int, dateEnd: LocalDate = new LocalDate): DataFrame = {
    val logPaths = datePaths(previousDays, "s3n://jiantest/mail/", "*/*.bz2", dateEnd)
    load(sqlContext, logPaths)
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

  def load(sqlContext: SQLContext, paths: Seq[String]): DataFrame =
    {
      val dataFrames: Seq[DataFrame] = paths.flatMap { path =>
        try {
          val dataFrame = sqlContext.jsonFile(path)
          Some(dataFrame)
        } catch {
          case e: Throwable =>
            //TODO log this exception properly!
            println(s"Unable to load $path")
            //e.printStackTrace()
            None
        }
      }
      dataFrames.reduce { (a, b) =>
        a.unionAll(b)
      }
    }

}
