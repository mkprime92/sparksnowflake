package com.sparkSnowFlake

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadDataFromSnowflake extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkSnowflakeRead")
    .getOrCreate()
  var sfParameters = Map(
    "sfURL" -> "https://re54891.east-us-2.azure.snowflakecomputing.com/",
    "sfAccount" -> "re54891",
    "sfUser" -> "mouhamadkeita92",
    "sfPassword" -> "**********",
    "sfDatabase" -> "PEOPLE",
    "sfSchema" -> "PUBLIC",
    "sfRole" -> "ACCOUNTADMIN"
  )

  val peopleDF: DataFrame = spark.read
    .format("snowflake")
    .options(sfParameters)
    .option("dbtable", "PEOPLE")
    .load()
  val peopleDFFilter: DataFrame = spark.read
    .format("snowflake")
    .options(sfParameters)
    .option("query", "SELECT * FROM PEOPLE WHERE job = 'Developer'")
    .load()
  peopleDF.show(false)
  peopleDFFilter.show(false)
}