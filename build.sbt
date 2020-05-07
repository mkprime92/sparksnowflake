name := "sparksnowflake"
version := "1.0"
scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
"org.apache.spark" % "spark-core_2.11" % "2.4.5",
"org.apache.spark" % "spark-sql_2.11" % "2.4.5",
"org.apache.spark" % "spark-streaming_2.11" % "2.4.5",
"org.apache.spark" % "spark-mllib_2.11" % "2.4.5",
"org.jmockit" % "jmockit" % "1.34" % "test",
  "net.snowflake" %% "spark-snowflake" % "2.7.0-spark_2.4"
)