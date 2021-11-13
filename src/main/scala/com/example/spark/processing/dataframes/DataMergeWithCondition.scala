package com.example.spark.processing.dataframes

import com.example.spark.processing.common.{CommonBase}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions._

case class UserBehaviourData(user_id: String, item_id: String, timestamp: String)

object DataMergeWithCondition extends Logging with CommonBase {
  def main(args: Array[String]): Unit = {
    // view data
    //    user_id item_id timestamp
    //    a   1   ts_1
    //    a   2   ts_2
    //    b   1   ts_1
    //    c   4   ts_3

    // purchase data
    //    user_id item_id timestamp
    //    a   2   ts_7
    //    a   2   ts_10
    //    c   2   ts_11

    // merged data with respective flags
    //    user_id item_id viewed_flag purchased_flag
    //    a   1   1   0
    //    b   1   1   0
    //    c   4   1   0
    //    a   2   1   1
    //    c   2   0   1

    // get sparkSession
    val spark = getSparkSession("mergeWithCondApp")

    // create dummy viewDF
    val viewData = Seq(
      UserBehaviourData("a", "1", "ts_1"),
      UserBehaviourData("a", "2", "ts_2"),
      UserBehaviourData("b", "1", "ts_1"),
      UserBehaviourData("c", "4", "ts_3")
    )
    var viewDF = spark.createDataFrame(spark.sparkContext.parallelize(viewData))
    viewDF.show

    // create dummy purchaseDF
    val purchaseData = Seq(
      UserBehaviourData("a", "2", "ts_7"),
      UserBehaviourData("a", "2", "ts_10"),
      UserBehaviourData("c", "2", "ts_11")
    )
    var purchaseDF = spark.createDataFrame(spark.sparkContext.parallelize(purchaseData))
    purchaseDF.show

    // drop timestamp column, add respective flags, remove duplicate rows
    viewDF = viewDF
      .drop("timestamp")
      .withColumn("viewed_flag", lit(1))
      .distinct()
    purchaseDF = purchaseDF
      .drop("timestamp")
      .withColumn("purchased_flag", lit(1))
      .distinct()

    // full outer join, replace null with 0
    val mergedDF = viewDF
      .join(purchaseDF, Seq("user_id", "item_id"), "outer")
      .na
      .fill(0)
    mergedDF.show(false)

    // sparkSession close
    spark.close
  }
}
