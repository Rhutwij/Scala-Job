package com.jobs2careers.base

import org.apache.spark.SparkContext

/**
 * This trait exposes a Spark Context and the Spark Configuration to anyone
 * mixing it in.
 */
trait RedisConfig{
  val BIG_DATA_REDIS_DB_HOST = "profiles-001.pnuura.0001.use1.cache.amazonaws.com"
  val BIG_DATA_REDIS_DB_PORT = 6379
}
