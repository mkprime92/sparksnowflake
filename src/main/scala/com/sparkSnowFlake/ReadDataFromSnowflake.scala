package com.sparkSnowFlake

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadDataFromSnowflake extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  var sfOptions = Map(
    "sfURL" -> "https://re54891.east-us-2.azure.snowflakecomputing.com/",
    "sfAccount" -> "re54891",
    "sfUser" -> "mouhamadkeita92",
    "sfPassword" -> "Ousmane92.",
    "sfDatabase" -> "EMPLOYEE",
    "sfSchema" -> "PUBLIC",
    "sfRole" -> "SYSADMIN"
  )

  val df: DataFrame = spark.read
    .format("snowflake")
    .options(sfOptions)
    .option("dbtable", "EMPLOYEE")
    .load()
  val df1: DataFrame = spark.read
    .format("snowflake")
    .options(sfOptions)
    .option("query", "select department, sum(salary) as total_salary from EMPLOYEE group by department")
    .load()

  df.show(false)
  df1.show(false)
}