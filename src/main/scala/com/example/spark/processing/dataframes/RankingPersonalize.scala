package com.example.spark.processing.dataframes

import com.example.spark.processing.common.CommonBase
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import scalapb.spark.Implicits.typedEncoderToEncoder
import scala.collection.parallel.immutable.ParSeq

case class UserAttributePatternC(user_id: String, attribute_1: String, attribute_3: Integer, attribute_2: String, pattern_name: String, pattern_id: Integer)
case class TransactionData(item_id: String, user_id: String, quantity: Int, timestamp: String)
object RankingPersonalize extends Logging with CommonBase {
  def main(args: Array[String]): Unit = {
    // transaction data
    //    item_id,user_id,quantity,timestamp
    //    item_1, user_1, 1, ts_1
    //    item_1, user_5, 2, ts_3
    //    item_2, user_1, 3, ts_4
    //    item_5, user_7, 1, ts_5
    //    item_6, user_10, 3, ts_6
    //    item_7, user_11, 1, ts_7
    //    item_8, user_12, 2, ts_9

    // user attribute pattern
    //    user_id, attribute_1, attribute_2, attribute_3, pattern_name, pattern_id
    //    user_1, apple, 1 , hoge, apple_1_hoge, 1
    //    user_2, apple, 2 , poge, apple_2_poge, 2
    //    user_3, mango, 1 , hoge, mango_1_hoge, 3
    //    user_4, mango, 2 , poge, mango_2_poge, 4
    //    user_5, apple, 1 , poge, apple_1_poge, 5
    //    user_6, mango, 1 , poge, mango_1_poge, 6
    //    user_7, apple, 2 , hoge, apple_2_hoge, 7
    //    user_8, mango, 2 , hoge, mango_2_hoge, 8
    //    user_9, apple, 1 , poge, apple_1_poge, 5
    //    user_10, apple, 2, hoge, apple_2_hoge, 7
    //    user_11, apple, 2, hoge, apple_2_hoge, 7
    //    user_12, apple, 2, null, null, null,
    //    user_13, apple, 2, hoge, apple_2_hoge, 7

    // ranking according to column if present or blank from args
    var rankingColumn = ""
    if (args.length > 1) {
      throw new Exception("Arguments exceeded, needed 1")
    } else if (args.length == 1) {
      logger.info(s"ranking will be according to column: ${args(0)}")
      rankingColumn = args(0)
    } else {
      logger.info("ranking will be according to records")
    }

    // get sparkSession
    val spark = getSparkSession("rankingPersonalize")

    // create dummy transactionData
    val transactionData = Seq(
      TransactionData("item_1", "user_1", 1, "ts_1"),
      TransactionData("item_1", "user_5", 2, "ts_3"),
      TransactionData("item_2", "user_1", 3, "ts_4"),
      TransactionData("item_5", "user_7", 1, "ts_5"),
      TransactionData("item_6", "user_10", 3, "ts_6"),
      TransactionData("item_7", "user_11", 1, "ts_7"),
      TransactionData("item_8", "user_12", 2, "ts_9"),
      TransactionData("item_7", "user_13", 2, "ts_9")
    )

    // create dummy userAttributePattern
    val userAttributePattern = Seq(
      UserAttributePatternC("user_1", "apple", 1 , "hoge", "apple_1_hoge", 1),
      UserAttributePatternC("user_2", "apple", 2 , "poge", "apple_2_poge", 2),
      UserAttributePatternC("user_3", "mango", 1 , "hoge", "mango_1_hoge", 3),
      UserAttributePatternC("user_4", "mango", 2 , "poge", "mango_2_poge", 4),
      UserAttributePatternC("user_5", "apple", 1 , "poge", "apple_1_poge", 5),
      UserAttributePatternC("user_6", "mango", 1 , "poge", "mango_1_poge", 6),
      UserAttributePatternC("user_7", "apple", 2 , "hoge", "apple_2_hoge", 7),
      UserAttributePatternC("user_8", "mango", 2 , "hoge", "mango_2_hoge", 8),
      UserAttributePatternC("user_9", "apple", 1 , "poge", "apple_1_poge", 5),
      UserAttributePatternC("user_10", "apple", 2, "hoge", "apple_2_hoge", 7),
      UserAttributePatternC("user_11", "apple", 2, "hoge", "apple_2_hoge", 7),
      UserAttributePatternC("user_12", "apple", 2, null, null, null),
      UserAttributePatternC("user_13", "apple", 2, "hoge", "apple_2_hoge", 7)
    )
    try {
      // create transactionDataDF
      val transactionDataDF = spark.createDataFrame(spark.sparkContext.parallelize(transactionData))
      // transactionDataDF.show

      // create userAttributePatternDF
      var userAttributePatternDF = spark.createDataFrame(spark.sparkContext.parallelize(userAttributePattern))
      // filter out null pattern_ids
      userAttributePatternDF = userAttributePatternDF
        .select("*")
      // userAttributePatternDF.show

      // 1. inner join transaction data with user attribute pattern on user_id
      val inputDF = transactionDataDF
        .join(userAttributePatternDF, Seq("user_id"), "inner")
      // inputDF.show

      // 2. make a list of distinct pattern_ids(0 identifies null or no-hit pattern from userAttributePatternDF)
      val patternIds = inputDF
        .select("pattern_id")
        .where(col("pattern_id").isNotNull)
        .distinct()
        .as[Int].collect.toList :+ 0

      // 3. parallel map against pattern_ids to get ParSeq[DataFrame]
      val personalizedDFs: ParSeq[DataFrame] = patternIds.par.map { id =>
        // when pattern_id inside inputDF then personalized ranking
        var resultDF = rankingColumn match {
          case "" =>
            inputDF
              .filter(s"pattern_id = ${id}")
              .groupBy("item_id", "pattern_id")
              .agg(count("item_id") as "score")
              .sort(desc("score"))
              .select("item_id", "pattern_id", "score")
          case s: String =>
            inputDF
              .filter(s"pattern_id = ${id}")
              .groupBy("item_id", "pattern_id")
              .agg(sum(inputDF(s)) as "score")
              .sort(desc("score"))
              .select("item_id", "pattern_id", "score")
        }
        // when the pattern_id is 0 means null(not in inputDF) then default ranking
        if (resultDF.isEmpty) {
          resultDF = rankingColumn match {
            case "" =>
              inputDF
                .groupBy("item_id")
                .agg(count("item_id") as "score")
                .sort(desc("score"))
                .withColumn("pattern_id", lit(id))
                .select("item_id", "pattern_id", "score")
            case s: String =>
              inputDF
                .groupBy("item_id")
                .agg(sum(inputDF(s)) as "score")
                .sort(desc("score"))
                .withColumn("pattern_id", lit(id))
                .select("item_id", "pattern_id", "score")
          }
        }
        // where pattern_id is 0 transform to null
        resultDF
          .withColumn("pattern_id", when(col("pattern_id") === 0, null)
          .otherwise(col("pattern_id")))
      }
      // union all dfs
      case class Output(item_id: String, pattern_id: Int, score: Int)
      val schema = StructType(Seq(
        StructField("item_id", StringType, nullable = false),
          StructField("pattern_id", IntegerType, nullable = true),
          StructField("score", IntegerType, nullable = false)))
      var outputDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      personalizedDFs.seq.foreach(df => {
        outputDF = df.union(outputDF)
      })
      outputDF.show(1000)
    } catch {
      case e: Throwable =>
        // sparkSession close
        spark.close
        throw new Exception(e)
    } finally {
      // sparkSession close
      spark.close
    }
  }
}
