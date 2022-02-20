package com.example.spark.processing.common

import com.example.spark.processing.config.Config
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession

trait CommonBase extends Logging {

  /**
    * SparkSession
    *
    * @return
    */
  def getSparkSession(appName: String): SparkSession = {
    // Init Spark
    val builder = SparkSession
      .builder()

    if (Config().getString("spark.master") != null && Config().getString("spark.master") != "") {
      builder.master((Config().getString("spark.master")))
    }

    val spark = builder
      .appName(appName)
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("checkpoint/")

    spark
  }
}
