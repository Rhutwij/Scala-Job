package com.jobs2careers.apps

import java.io.File
import java.net.URL

import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.WordSpec

import com.jobs2careers.utilities.SharedSparkContext
import com.jobs2careers.utils.TempFiles

/**
 * This test is written in a style called WordSpec (as opposed to FunSpec)
 * A comprehensive list is here:
 * http://scalatest.org/user_guide/selecting_a_style
 */
class SaveAndLoadRDDJobSpec extends WordSpec with SharedSparkContext {

  "SaveAndLoadRDDJob" when {
    "saving an RDD to the filesystem with saveRDD" should {
      "save an RDD to the filesystem" in {
        SaveAndLoadRDDJob.saveRDD(sc)
        val path = new URL(SaveAndLoadRDDJob.pathToFile).getPath
        val filesystemRDD = new File(path)
        try {
          // Verify some things
          filesystemRDD.exists() shouldBe true
        } finally {
          // Delete the file we made by calling saveRDD. Tests should have no
          // side-effects.
          TempFiles.deleteRecursive(filesystemRDD)
        }

      }
    }

    "transforming an RDD" should {
      "turn an RDD of Strings into an RDD of Integers" in {

        // val result = SaveAndLoadRDDJob.plusOne()
      }
    }
  }

}
