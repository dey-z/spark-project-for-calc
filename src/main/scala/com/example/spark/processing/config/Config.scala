package com.example.spark.processing.config

import com.typesafe.config._

object Config {
  def apply(): Config = ConfigFactory.load()
}
