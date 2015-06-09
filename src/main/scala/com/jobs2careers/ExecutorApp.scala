package com.jobs2careers

import com.jobs2careers.base.SparkBaseApp
import org.slf4j.LoggerFactory

import scala.reflect.runtime._

/**
 * Created by carl on 6/9/15.
 */
object ExecutorApp extends App{

  val log = LoggerFactory.getLogger("ExecutorApp")

  log.info("Start to Spark tasks!")

  val className = args.size match  {
    case 1 =>{
      args(0).toString
    }
    case x:Int if (x > 2) => {
      args(0).toString
    }
    case _ => {
      log.error("No args, system will just exit.")
      sys.exit()
    }
  }

  val mirror = universe.runtimeMirror(getClass.getClassLoader)
  val classInstance = Class.forName(className)
  val task = classInstance.newInstance().asInstanceOf[SparkBaseApp]
  task.executeTask(args)

}