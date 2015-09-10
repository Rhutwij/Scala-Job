package com.jobs2careers.base

import com.typesafe.config.ConfigFactory

/**
 * This trait exposes a Spark Context and the Spark Configuration to anyone
 * mixing it in.
 */
trait RedisConfig{
  val conf = ConfigFactory.load()
  val BIG_DATA_REDIS_DB_HOST = conf.getString("redis.host")
  val BIG_DATA_REDIS_DB_PORT = conf.getInt("redis.port")
}