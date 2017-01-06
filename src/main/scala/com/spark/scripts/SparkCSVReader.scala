package com.spark.scripts

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Logger,Level}
import java.util.Calendar

/**
  * Created by Prashant on 2017-01-06.
  */
object SparkCSVReader {
  def main(args: Array[String]): Unit= {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val today = Calendar.getInstance().getTime
    println(s"Script executed on $today")

    /*
    This code can be re-written to use SparkSession objects coz as of
    Spark 2.0 alot of methods have been depricated as future of Spark
    moves with dataframes being default abstraction as opposed to RDDs.
     */

    val data_path = "/Users/Prashant/githubrepo/spark-scala/src/main/resources/"
    val conf = new SparkConf().setAppName("SparkCSVReader").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val auctionDF = sqlContext.
                    read.
                    format("com.databricks.spark.csv").
                    option("header", "true").
                    option("inferSchema", "true").
                    load(data_path + "ebay.csv")


    // How many auctions were held?
    val count = auctionDF.select("auctionid").distinct.count
    println("Distinct items : " + count)

    // How many bids per item?
    auctionDF.groupBy("auctionid", "item").count.sort("auctionid").show

    // What's the min number of bids per item? what's the average? what's the max?
    auctionDF.groupBy("item", "auctionid").count.agg(min("count"), avg("count"), max("count")).show

    // Get the auctions with closing price > 100
    auctionDF.filter("price > 100").sort("auctionid").show

    // register the DataFrame as a temp table
    auctionDF.registerTempTable("auction")

    // SQL statements can be run
    // How many  bids per auction?
    val results = sqlContext.sql("SELECT auctionid, item,  count(bid) as BidCount FROM auction GROUP BY auctionid, item")

    // display dataframe in a tabular format
    results.sort("auctionid").show()

    sqlContext.sql("SELECT auctionid,item, MAX(price) as MaxPrice FROM auction  GROUP BY item,auctionid").sort("auctionid").show()
  }
}
