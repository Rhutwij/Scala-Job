package com.jobs2careers.utils

/**
 * @author rtulankar

 */
import org.apache.commons.codec.digest.DigestUtils
trait HashFunctions {
  def md5(str:String):String={
    DigestUtils.md5Hex(str)
  }
}