package com.jobs2careers.apps

import com.jobs2careers.base.SparkBaseJob
import com.jobs2careers.utils.{ LogLike, TempFiles }
import org.slf4j.LoggerFactory
import scala.collection.Map
import org.apache.spark.rdd.RDD

/**
 * This is a sample Spark job that counts animals in a given file. Each animal
 * is one line in the file.
 */
object CountAnimalsJob extends SparkBaseJob with LogLike {

  /**
   * Called by spark-submit and sbt run.
   * @param args the arguments for this job.
   *             [path://to/file] or else use a temp file.
   */
  def main(args: Array[String]): Unit = {
    args match {
      case Array(first, _*) =>
        println(getAnimalCounts(first))
      case Array(_*) =>
        println("No file path provided! Using a temp file.")
        val tempFile = TempFiles.createTempFile(
          "animals.txt", Seq("cat", "dog", "shark", "tiger").mkString("\n")
        )
        val results = getAnimalCounts(tempFile.getAbsolutePath)
        println(resultsBanner)
        println(results)
    }
  }

  /**
   * Counts the animals in the animals file.
   * @return a map of animal -> count
   */
  def getAnimalCounts(pathToAnimals: String): Map[String, Long] = {
    // Create an RDD from a file containing animal types.
    val sc = getSparkContext
    val rdd = sc.textFile(pathToAnimals)

    // Group by the type of animal.
    val result = groupByAnimalType(rdd)
    sc.stop()

    logger.info(s"Wow, ${result.size} kinds of animals!")
    result
  }

  def groupByAnimalType(animals: RDD[String]) = {
    // Group by the type of animal.
    animals.countByValue()
  }
}
