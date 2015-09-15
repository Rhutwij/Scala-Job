package com.jobs2careers.utilities

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

/**
 * From https://github.com/apache/spark/blob/4a462c282c72c47eeecf35b4ab227c1bc71908e5/core/src/test/scala/org/apache/spark/SharedSparkContext.scala
 * @author jacob
 */

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>

  // also check out Spark testing at
  // https://spark-summit.org/2014/wp-content/uploads/2014/06/Testing-Spark-Best-Practices-Anupama-Shetty-Neil-Marshall.pdf
  // http://www.quantifind.com/blog/2013/01/unit-testing-with-spark/

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  var conf = new SparkConf(false)

  override def beforeAll() {
    killJvmSparkContext()
    _sc = new SparkContext("local[4]", "test", conf)
    super.beforeAll()
  }

  override def afterAll() {
    killJvmSparkContext()
    super.afterAll()
  }

  def killJvmSparkContext() = {
    if (_sc != null) {
      _sc.stop

      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")

      _sc = null
    }
  }
}