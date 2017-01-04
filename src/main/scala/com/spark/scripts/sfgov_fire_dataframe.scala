package com.spark.scripts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Logger,Level}
import java.util.Calendar

/**
  * Created by Prashant on 2017-01-02.
  */
object sfgov_fire_dataframe {
  def main(args: Array[String]): Unit={

    Logger.getLogger("org").setLevel(Level.ERROR)

    //Due to the large size of the file, it will not be available on github account
    //However, public dataset can be downloaded from following link as csv
    //https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3

    val data_path = "/Users/Prashant/githubrepo/spark-scala/src/main/resources/"

    val spark = SparkSession.
                builder().
                master("local[*]").
                getOrCreate()

    //Importing implicits for easy sorting later on in the code
    import spark.implicits._

    val today = Calendar.getInstance().getTime

    println(s"Script executed on $today")

    //CSV file has space in column names. Here we are creating column names without spaces
    //Also if our data set is large with many columns, creating and specifying schema
    //provides a performance benefit

    val fireSchema = StructType(StructField("CallNumber", IntegerType, true) ::
                                StructField("UnitID", StringType, false) ::
                                StructField("IncidentNumber", IntegerType, true) ::
                                StructField("CallType", StringType, true) ::
                                StructField("CallDate", StringType, true) ::
                                StructField("WatchDate", StringType, true) ::
                                StructField("ReceivedDtTm", StringType, true) ::
                                StructField("EntryDtTm", StringType, true) ::
                                StructField("DispatchDtTm", StringType, true) ::
                                StructField("ResponseDtTm", StringType, true) ::
                                StructField("OnSceneDtTm", StringType, true) ::
                                StructField("TransportDtTm", StringType, true) ::
                                StructField("HospitalDtTm", StringType, true) ::
                                StructField("CallFinalDisposition", StringType, true) ::
                                StructField("AvailableDtTm", StringType, true) ::
                                StructField("Address", StringType, true) ::
                                StructField("City", StringType, true) ::
                                StructField("ZipcodeofIncident", IntegerType, true) ::
                                StructField("Battalion", StringType, true) ::
                                StructField("StationArea", StringType, true) ::
                                StructField("Box", StringType, true) ::
                                StructField("OriginalPriority", StringType, true) ::
                                StructField("Priority", StringType, true) ::
                                StructField("FinalPriority", IntegerType, true) ::
                                StructField("ALSUnit", BooleanType, true) ::
                                StructField("CallTypeGroup", StringType, true) ::
                                StructField("NumberofAlarms", IntegerType, true) ::
                                StructField("UnitType", StringType, true) ::
                                StructField("Unitsequenceincalldispatch", IntegerType, true) ::
                                StructField("FirePreventionDistrict", StringType, true) ::
                                StructField("SupervisorDistrict", StringType, true) ::
                                StructField("NeighborhoodDistrict", StringType, true) ::
                                StructField("Location", StringType, true) ::
                                StructField("RowID", StringType, true) ::
                                Nil
                               )

    val fireServiceCallsDF =  spark.
                              read.
                              option("header","true").
                              schema(fireSchema).
                              csv(data_path + "Fire_Department_Calls_for_Service.csv")

    //First 5 records in fireServiceCallsDF dataframe
    fireServiceCallsDF.show(5,false)

    //Get total number of records in dataframe
    println("Total Number of records in dataframe : " + fireServiceCallsDF.count())

    //How many different type of calls were made to Fire Department?
    //The False below expands the ASCII column width to fit the full text in the output
    fireServiceCallsDF.select("CallType").distinct().show(35,false)

    //How many incidents of each type were there?
    fireServiceCallsDF.select("CallType").groupBy("CallType").count().orderBy($"count".desc).show(35,false)


    /*
    Further we will explore Date/Time Analysis.
    However currently our Date/Time columns are of String Type
    Using unix_timestamp we will create new DF with timestamp columns
    */

    //File had two formats of date/time values
    val from_pattern1 = "MM/dd/yyyy"
    val from_pattern2 = "MM/dd/yyyy hh:mm:ss aa"

    val fireServiceCallsTsDF =  fireServiceCallsDF.
                                withColumn("CallDateTS", unix_timestamp(fireServiceCallsDF("CallDate"), from_pattern1).
                                  cast("timestamp")).
                                  drop("CallDate").
                                withColumn("WatchDateTS", unix_timestamp(fireServiceCallsDF("WatchDate"), from_pattern1).
                                  cast("timestamp")).
                                  drop("WatchDate").
                                withColumn("ReceivedDtTmTS", unix_timestamp(fireServiceCallsDF("ReceivedDtTm"), from_pattern2).
                                  cast("timestamp")).
                                  drop("ReceivedDtTm").
                                withColumn("EntryDtTmTS", unix_timestamp(fireServiceCallsDF("EntryDtTm"), from_pattern2).
                                  cast("timestamp")).
                                  drop("EntryDtTm").
                                withColumn("DispatchDtTmTS", unix_timestamp(fireServiceCallsDF("DispatchDtTm"), from_pattern2).
                                  cast("timestamp")).
                                  drop("DispatchDtTm").
                                withColumn("ResponseDtTmTS", unix_timestamp(fireServiceCallsDF("ResponseDtTm"), from_pattern2).
                                  cast("timestamp")).
                                  drop("ResponseDtTm").
                                withColumn("OnSceneDtTmTS", unix_timestamp(fireServiceCallsDF("OnSceneDtTm"), from_pattern2).
                                  cast("timestamp")).
                                  drop("OnSceneDtTm").
                                withColumn("TransportDtTmTS", unix_timestamp(fireServiceCallsDF("TransportDtTm"), from_pattern2).
                                  cast("timestamp")).
                                  drop("TransportDtTm").
                                withColumn("HospitalDtTmTS", unix_timestamp(fireServiceCallsDF("HospitalDtTm"), from_pattern2).
                                  cast("timestamp")).
                                  drop("HospitalDtTm").
                                withColumn("AvailableDtTmTS", unix_timestamp(fireServiceCallsDF("AvailableDtTm"), from_pattern2).
                                  cast("timestamp")).
                                  drop("AvailableDtTm")

    //How many years of Fire Service calls is in the data file?
    val noOfYearsOfData = fireServiceCallsTsDF.select(year($"CallDateTS")).distinct().count()

    println(s"Data File contains $noOfYearsOfData years of Fire Service Calls")

    //How many service calls were logged in the past 7 days?
    fireServiceCallsTsDF.
      filter(year($"CallDateTS") === "2016").
      filter(dayofyear($"CallDateTS") >= 359).
      groupBy(dayofyear($"CallDateTS")).
      count().
      orderBy("dayofyear(CallDateTS)").
      show()


  }
}
