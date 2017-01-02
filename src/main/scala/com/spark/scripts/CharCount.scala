package com.spark.scripts

/**
  * Created by Prashant on 2017-01-01.
  * This Script accepts file as command line argument on data_path
  * and outputs character count for each letter in specified file.
  */

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j.{Logger,Level}
import java.util.Calendar

object CharCount {
  def main(args: Array[String]): Unit={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("Spark Word Count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val today = Calendar.getInstance().getTime

    println(s"Script executed on $today")

    //Default file path for all the scripts
    val data_path = "/Users/Prashant/githubrepo/spark-scala/src/main/resources/"

    val threshold = 2

    // Accept and flatten document into list of words
    // val tokenized = sc.textFile(data_path + args(0)).flatMap(_.split(" "))
    val tokenized = sc.textFile(data_path + args(0)).flatMap(word => word.split(" "))

    // count the occurrence of each word
    // val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    val wordCounts = tokenized.map(word => (word, 1)).reduceByKey((v1,v2) => v1 + v2)

    // filter out words with less than threshold occurrences
    // val filtered = wordCounts.filter(_._2 >= threshold)
    val filtered = wordCounts.filter(line => line._2 >= threshold)

    // count characters
    // val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
    val charCounts = filtered.flatMap(line => line._1.toCharArray).map(char => (char, 1)).reduceByKey((v1,v2) => v1 + v2)

    println("---------------------------------------------------")
    println(charCounts.collect().mkString(", "))

  }
}
