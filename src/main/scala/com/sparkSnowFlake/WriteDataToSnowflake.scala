package com.sparkSnowFlake

import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteDataToSnowflake extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(("James","Sales",3000),
    ("Michael","Sales",4600),
    ("Robert","Sales",4100),
    ("Maria","Finance",3000),
    ("Raman","Finance",3000),
    ("Scott","Finance",3300),
    ("Jen","Finance",3900),
    ("Jeff","Marketing",3000),
    ("Kumar","Marketing",2000)
  )
  val df = simpleData.toDF("name","department","salary")
  df.show()

  var sfOptions = Map(
    "sfURL" -> "https://re54891.east-us-2.azure.snowflakecomputing.com/",
    "sfAccount" -> "re54891",
    "sfUser" -> "mouhamadkeita92",
    "sfPassword" -> "Ousmane92.",
    "sfDatabase" -> "EMPLOYEE",
    "sfSchema" -> "PUBLIC",
    "sfRole" -> "SYSADMIN"
  )

  df.write
    .format("snowflake")
    .options(sfOptions)
    .option("dbtable", "EMPLOYEE")
    .mode(SaveMode.Overwrite)
    .save()

}
