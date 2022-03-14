package com.example.spark.processing.dataframes

import com.example.spark.processing.common.CommonBase
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

case class UserData(user_id: String, attribute_1: String, attribute_2: String)
object UserAttributePattern extends Logging with CommonBase {
  def main(args: Array[String]): Unit = {
    // specify attributes column(max = 3)
    val attributes = Array("attribute_1", "attribute_2")

    // customer/user data
    //    user_id	attribute_1	attribute_2
    //    user_1 apple 1
    //    user_2 apple 2
    //    user_4 mango 2
    //    user_5 apple 1
    //    user_6 mango 1
    //    user_7 apple 2
    //    user_8 mango 2
    //    user_9 apple 1
    //    user_10 apple 2
    //    user_11 apple 2
    //    user_12 apple 2
    //    user_13 apple 2

    // get sparkSession
    val spark = getSparkSession("userAttributePattern")

    // create dummy userData
    val userData = Seq(
      UserData("user_1", "りんご", "1"),
      UserData("user_2", "apple", "2"),
      UserData("user_4", "mango", "2"),
      UserData("user_5", "apple", "1"),
      UserData("user_5", "apple", "2"),
      UserData("user_5", "apple", "3"),
      UserData("user_6", "mango", "1"),
      UserData("user_7", "apple", "2"),
      UserData("user_8", "mango", "2"),
      UserData("user_9", "apple", "1"),
      UserData("user_10", "apple", "2"),
      UserData("user_11", "apple", "2"),
      UserData("user_12", null, null),
      UserData("user_13", "apple", "2")
    )
    try {
      // create userDataDF
      var userDataDF = spark.createDataFrame(spark.sparkContext.parallelize(userData))
      // userDataDF.show

      // patternize userData -> userAttributePattern
      // +-------+-----------+-----------+------------+----------+
      // |user_id|attribute_1|attribute_2|pattern_name|pattern_id|
      // +-------+-----------+-----------+------------+----------+
      // |user_5 |apple      |1          |apple_1     |1         |
      // |user_9 |apple      |1          |apple_1     |1         |
      // |user_12|null       |null       |null        |null      |
      // |user_1 |りんご     |1          |りんご_1    |5         |
      // |user_6 |mango      |1          |mango_1     |3         |
      // |user_4 |mango      |2          |mango_2     |4         |
      // |user_8 |mango      |2          |mango_2     |4         |
      // |user_2 |apple      |2          |apple_2     |2         |
      // |user_7 |apple      |2          |apple_2     |2         |
      // |user_10|apple      |2          |apple_2     |2         |
      // |user_11|apple      |2          |apple_2     |2         |
      // |user_13|apple      |2          |apple_2     |2         |
      //  +-------+-----------+-----------+------------+----------+

      // 1. add pattern_name column using specified attributes
      if (attributes.length > 3) {
        throw new Exception("max attributes is 3")
      } else if (attributes.length == 3) {
        userDataDF = userDataDF.withColumn(
          "pattern_name",
          concat(
            userDataDF.col(s"${attributes(0)}"),
            concat(lit("_")),
            concat(userDataDF.col(s"${attributes(1)}"), concat(lit("_")), concat(userDataDF.col(s"${attributes(2)}")))
          )
        )
      } else if (attributes.length == 2) {
        userDataDF = userDataDF.withColumn(
          "pattern_name",
          concat(userDataDF.col(s"${attributes(0)}"), concat(lit("_")), concat(userDataDF.col(s"${attributes(1)}")))
        )
      } else {
        userDataDF = userDataDF.withColumn("pattern_name", col(s"${attributes(0)}"))
      }
      // userDataDF.show()

      // remove Duplicates of customers/users
      userDataDF = userDataDF
        .dropDuplicates("user_id")
      // userDataDF.show()

      // 2. add pattern_id using row_number
      val listCols = userDataDF.columns.toList ++ List("pattern_id")
      logger.info(listCols)
      val userDataAttributePatternDF = userDataDF
        .select("pattern_name")
        .distinct()
        .where(col("pattern_name").isNotNull)
        .select(
          row_number().over(Window.orderBy("pattern_name")) as "pattern_id",
          userDataDF("pattern_name")
        )
        .join(userDataDF, Seq("pattern_name"), "outer")
        .select(listCols.map(m=>col(m)):_*)
      userDataAttributePatternDF.show(1000)
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
