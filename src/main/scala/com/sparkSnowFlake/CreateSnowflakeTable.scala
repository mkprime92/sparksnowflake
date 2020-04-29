package com.sparkSnowFlake

import java.sql.DriverManager

object CreateSnowflakeTable extends App {

  val properties = new java.util.Properties()
  properties.put("user", "mouhamadkeita92")
  properties.put("password", "Ousmane92.")
  properties.put("account", "re54891")
  properties.put("warehouse", "COMPUTE_WH")
  properties.put("db", "DEMO_DB")
  properties.put("schema", "public")
  properties.put("role", "SYSADMIN")

  val jdbcUrl = "jdbc:snowflake://re54891.east-us-2.azure.snowflakecomputing.com/"

  println("Created JDBC connection")
  val connection = DriverManager.getConnection(jdbcUrl, properties)
  println("Done creating JDBC connection")

  println("Created JDBC statement")
  val statement = connection.createStatement

  // create a table
  println("Creating table EMPLOYEE")
  statement.executeUpdate("create or replace table EMPLOYEE(name VARCHAR, department VARCHAR, salary number)")
  statement.close()
  println("Done creating EMPLOYEE table")

  connection.close()
  println("End connection")
}
