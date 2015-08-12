package com.jobs2careers.apps

import java.io.File
import java.net.URL
import com.jobs2careers.base.SparkBaseJob
import com.jobs2careers.utils.{TempFiles, LogLike}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.nio.file.Files

/**
 * This job is an example of creating an RDD, saving it, and then loading it
 * and transforming it.
 */
object SaveAndLoadRDDJob extends SparkBaseJob with LogLike {
  // this is bad; should be configurable
  val tempDirPath = Files.createTempDirectory("AllSpark")
  Files.delete(tempDirPath)
  var pathToFile: String = tempDirPath.toUri().toASCIIString()
  
  /**
   * Called by spark-submit and sbt run.
   * @param args the arguments for this job.
   *             [path://to/file] or else use a temp file.
   */
  def main(args: Array[String]): Unit = {
    args match {
      case Array(first, _*) =>
        println(s"Using file from $first")
        pathToFile = first
      case _ =>
        println(s"Using default file $pathToFile")
    }
    // Get a SparkContext and save out the RDD
    val sc = getSparkContext
    saveRDD(sc)

    // Load the RDD and transform it
    val rdd = loadRDD(sc, pathToFile)
    val transformedRDD = plusOne(rdd)

    // Collect the results (sometimes the data is too large to do this).
    // You'll notice the elements are out of order. That's because that's
    //   the order in which they were collected from the executors.
    val results = transformedRDD.collect().toList

    // Let Spark know we're done now
    sc.stop()

    println(resultsBanner)
    println(results)

    // Delete the file we made
    val rddFile = new File(new URL(pathToFile).getPath)
    TempFiles.deleteRecursive(rddFile)
  }

  /**
   * Save out an RDD. The file structure is usually something like:
   * /path/to/myRDD/[part0, part1, part2 ... partN]
   * @param sc the SparkContext for our job
   */
  def saveRDD(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(1 to 10)
    rdd.saveAsTextFile(pathToFile)
  }

  /**
   * Load an RDD from a text file.
   * @param sc the SparkContext for our job
   * @return the RDD we loaded
   */
  def loadRDD(sc: SparkContext, path: String): RDD[String] = {
    sc.textFile(path)
  }

  /**
   * Given an RDD, add one to each element.
   * @param strings the RDD to transform
   * @return the transformed RDD
   */
  def plusOne(strings: RDD[String]): RDD[Int] = {
    strings.map(s => s.toInt + 1)
  }
}
