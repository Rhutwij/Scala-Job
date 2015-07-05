package com.jobs2careers.apps

import java.io.File
import com.jobs2careers.utilities.ClassPathResourceLoader
import org.scalatest.{ BeforeAndAfter, FunSpec }
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkContext
import com.jobs2careers.base.SparkLocalConfig
import org.apache.spark.rdd.RDD
import com.jobs2careers.utilities.SharedSparkContext

/**
 * Example ScalaTest
 *
 * Unit tests are typically named $CLASS_NAME+Spec
 * Integration tests are typically named $CLASS_NAME+ForIntegrationSpec
 *
 * Typically describe(){} blocks are used to organize the class and method
 * level while it(){} blocks are used for each method behavior test.
 *
 * If you use a SLF4J logger in your classes, don't expect to see the logs here.
 * For some reason ScalaTest swallows all the logs.
 *
 * You can run tests by right clicking on the class, or any of the it or
 * describe blocks.
 */
class CountAnimalsAppSpec extends FunSpec with MockitoSugar with BeforeAndAfter with SharedSparkContext {
  private val fixture = "fixtures/animals.txt"
  private var animals: RDD[String] = _

  before {
    val resource = new ClassPathResourceLoader()
      .loadResource(fixture)
    assert(resource != None, s"Test fixture $fixture does not exist!")
    val animalsFile = resource.get
    animals = sc.textFile(animalsFile.getPath)
  }

  describe("CountAnimalsApp") {
    describe("#getAnimalCounts") {
      it("should return the correct count for sharks") {
        val animalCounts = CountAnimalsJob.groupByAnimalType(animals)
        val expected = 2

        // it just so happens that there should be 2 sharks.
        animalCounts.getOrElse("shark", 0) should be(expected)
      }

      it("should return the correct count for deer") {
        val animalCounts = CountAnimalsJob.groupByAnimalType(animals)
        val expected = 3

        // it just so happens that there should be 3 deer.
        animalCounts.getOrElse("deer", 0) should be(expected)
      }

      // ... any number of it blocks ...
    }

    // ... any number of describe blocks
  }

}
