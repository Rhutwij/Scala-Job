package com.jobs2careers.apps

import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object DataRegistry {

  val fmt = DateTimeFormat.forPattern("yyyy/MM/dd/");

  def datePaths(days: Integer, prefix: String, suffix: String, datetime: LocalDate = new LocalDate()): String =
    {
      val previousDays = 1 to days
      val previousDatePaths: Seq[String] = previousDays map { a =>
        val prevdate = datetime.minusDays(a)
        val datestring = fmt.print(prevdate)
        val fixture = prefix + datestring + suffix
        fixture
      }

      val hadoopPath: String = previousDatePaths.reduce { (a, b) => s"$a,$b" }

      hadoopPath
    }

}