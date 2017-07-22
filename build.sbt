name := "metrics-app"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.1.0" % "provided",
  ("com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M3").exclude("io.netty", "netty-handler"),
  ("org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.0").exclude("org.spark-project.spark", "unused"),
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
