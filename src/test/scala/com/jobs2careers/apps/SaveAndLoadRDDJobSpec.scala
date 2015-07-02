//package com.jobs2careers.apps
//
//import java.io.File
//import java.net.URL
//
//import com.jobs2careers.utils.TempFiles
//import org.scalatest.Matchers._
//import org.scalatest.mock.MockitoSugar
//import org.scalatest.{BeforeAndAfter, WordSpec}
//import org.mockito.Mockito._
//
///**
// * This test is written in a style called WordSpec (as opposed to FunSpec)
// * A comprehensive list is here:
// * http://scalatest.org/user_guide/selecting_a_style
// */
//class SaveAndLoadRDDJobSpec extends WordSpec with MockitoSugar
//  with BeforeAndAfter {
//
//  val sc = SaveAndLoadRDDJob.getSparkContext
//
//  "SaveAndLoadRDDJob" when {
//    "saving an RDD to the filesystem with saveRDD" should {
//      "save an RDD to the filesystem"  in {
//        SaveAndLoadRDDJob.saveRDD(sc)
//        val path = new URL(SaveAndLoadRDDJob.pathToFile).getPath
//        val filesystemRDD = new File(path)
//
//        // Verify some things
//        filesystemRDD.exists() shouldBe true
//
//        // Delete the file we made by calling saveRDD. Tests should have no
//        // side-effects.
//        TempFiles.deleteRecursive(filesystemRDD)
//      }
//    }
//
//    "transforming an RDD" should {
//      "turn an RDD of Strings into an RDD of Integers" in {
//
//       // val result = SaveAndLoadRDDJob.plusOne()
//      }
//    }
//  }
//
//  after {
//    sc.stop()
//  }
//}
