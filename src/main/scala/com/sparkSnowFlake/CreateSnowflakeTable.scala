package com.sparkSnowFlake

import java.sql.DriverManager

object CreateSnowflakeTable extends App {

  val properties = new java.util.Properties()
  properties.put("user", "mouhamadkeita92")
  properties.put("password", "**********")
  properties.put("account", "re54891")
  properties.put("warehouse", "COMPUTE_WH")
  properties.put("db", "PEOPLE")
  properties.put("schema", "public")
  properties.put("role", "SYSADMIN")

  val jdbcUrl = "jdbc:snowflake://re54891.east-us-2.azure.snowflakecomputing.com/"

  println("Created connection")
  val connection = DriverManager.getConnection(jdbcUrl, properties)
  val statement = connection.createStatement
  println("Done creating connection")

  println("Creating table PEOPLE")
  statement.executeUpdate("create or replace table PEOPLE(name VARCHAR, age INT, job VARCHAR)")
  statement.close()
  println("Done creating PEOPLE table")

  connection.close()
}