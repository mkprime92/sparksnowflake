package com.sparkSnowFlake

import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteDataToSnowflake extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkSnowflakeWrite")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  var sfParameters = Map(
    "sfURL" -> "https://re54891.east-us-2.azure.snowflakecomputing.com/",
    "sfAccount" -> "re54891",
    "sfUser" -> "mouhamadkeita92",
    "sfPassword" -> "**********",
    "sfDatabase" -> "PEOPLE",
    "sfSchema" -> "PUBLIC",
    "sfRole" -> "ACCOUNTADMIN"
  )

  val data = Seq(("Jorge",23,"Developer"),
    ("Bob",28,"Developer"),
    ("Bill",31,"Admin"),
    ("John",40,"Project Manager")
  )
  val peopleDF = data.toDF("name","age","job")

  peopleDF.write
    .format("snowflake")
    .options(sfParameters)
    .option("dbtable", "PEOPLE")
    .mode(SaveMode.Overwrite)
    .save()

}
