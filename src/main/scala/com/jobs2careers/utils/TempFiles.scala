package com.jobs2careers.utils

import java.io.{FileWriter, File}

object TempFiles {

  /**
   * Creates a temporary file.
   * @return a file
   */
  def createTempFile(fileName: String, contents: String): File = {
    val file = File.createTempFile(fileName, ".tmp")
    file.setWritable(true)
    file.deleteOnExit()

    val writer = new FileWriter(file)
    writer.write(contents)
    writer.flush()
    writer.close()

    file
  }

  /**
   * Deletes a file and recursively deletes directories. Hopefully there's
   * enough stack space, or else we'll have to make this tail-recursive.
   *
   * Don't pass in your home directory.
   * @param file the file or directory to delete
   */
  def deleteRecursive(file: File): Unit = {
    file.isDirectory match {
      case true =>
        file.listFiles().map(deleteRecursive) // deletes the children
        file.delete()                         // deletes the parent
      case false =>
        file.delete()
    }
  }
}
