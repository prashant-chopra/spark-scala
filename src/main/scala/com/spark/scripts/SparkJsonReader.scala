package com.spark.scripts

import org.apache.spark.sql.SparkSession
import java.util.Calendar
import org.apache.log4j.{Level, Logger}

/**
  * Created by Prashant on 2017-01-05.
  */

object SparkJsonReader {
  def main(args: Array[String]): Unit= {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val data_path = "/Users/Prashant/githubrepo/spark-scala/src/main/resources/"

    val spark = SparkSession.
                builder().
                appName("Spark_JSON_Reader").
                master("local[*]").
                getOrCreate()

    val today = Calendar.getInstance().getTime
    println(s"Script executed on $today")

    //Provide appropriate file JSON file
    val salesDF = spark.read.json(data_path + "sales.json")

    salesDF.createOrReplaceTempView("sales")

    val aggDF = spark.sql("select sum(amountPaid) from sales")
    println(aggDF.collectAsList())

    val results = spark.sql("SELECT customerId,itemName FROM sales ORDER BY itemName")

    // display dataframe in a tabular format
    results.show()
  }
}
