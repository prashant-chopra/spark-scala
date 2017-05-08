package com.spark.scripts

/**
  * This is primarily to get the word count on the data received from
  * nc -lk 44444
  * Make sure build.sbt is updated with the dependency -
  * libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.2"
  * Create jar, ship the jar, start nc, and then use spark-submit
  * spark-submit --class StreamingSmokeTest --master yarn --conf spark.ui.port=14562 flukas_2.10-1.0.jar
  */

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._

object StreamingSmokeTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Streaming Smoke Test").setMaster("yarn-client")
    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.socketTextStream("gw01.mycluster.com", 44444)
    val linesFlatMap = lines.flatMap(rec => rec.split(" "))
    val linesMap =  linesFlatMap.map((_, 1))
    val linesRBK = linesMap.reduceByKey(_ + _)

    linesRBK.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
