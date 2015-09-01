package com.jobs2careers.base

/**
 * This trait exposes a Spark Context and the Spark Configuration to anyone
 * mixing it in.
 */
trait RedisConfig{
  val BIG_DATA_REDIS_DB_HOST = "profiles-ha.pnuura.ng.0001.use1.cache.amazonaws.com"
  val BIG_DATA_REDIS_DB_PORT = 6379
}
