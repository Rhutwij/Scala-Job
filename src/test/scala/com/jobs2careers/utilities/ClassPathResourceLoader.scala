package com.jobs2careers.utilities

import java.io.File

class ClassPathResourceLoader {

  def loadResource(fileName: String): Option[File] = {
    val classLoader = this.getClass.getClassLoader
    classLoader.getResource(fileName) match {
      case null =>
        None
      case url =>
        val file = new File(url.getPath)
        Some(file)
    }
  }

}
