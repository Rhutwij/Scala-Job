package com.jobs2careers.apps

import java.io.File

import com.jobs2careers.base.SparkBaseApp
import org.slf4j.LoggerFactory

import scala.collection.Map

/**
 * This is a sample Spark job that counts animals in a given file. Each animal
 * is one line in the file.
 * @param animals a file containing animals
 */
class CountAnimalsApp(animals: File) extends SparkBaseApp {
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  /**
   *
   * @return
   */
  def getAnimalCounts: Map[String, Long] = {
    // Create an RDD from a file containing animal types.
    val rdd = sc.textFile(animals.getPath)

    // Group by the type of animal.
    val result = rdd.countByValue()

    // Stop the Spark Context.
    sc.stop()

    logger.info(s"Wow, ${result.size} kinds of animals!")
    result
  }

}
