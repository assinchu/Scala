name := "ScalaDemo"

version := "0.1"

scalaVersion := "2.12.1"


lazy val buildSettings = Seq(
  name := "meta_add",
  version := "0.1",
  scalaVersion := "2.12.1",
  organization := "com.ge.fdl.meta"
)

val app = (project in file(".")).
  settings(buildSettings: _*)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-hive" % "2.4.4",
  "org.apache.logging.log4j" % "log4j-api" % "2.12.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.12.1",
  "mysql" % "mysql-connector-java" % "5.1.16",
  "com.typesafe.play" % "play-json_2.12" % "2.7.3",
  "net.liftweb" % "lift-json_2.12" % "3.2.0-M1",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.9.8",


)