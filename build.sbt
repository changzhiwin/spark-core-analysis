name := "spark-core-analysis"
organization := "xyz.sourcecodestudy.spark" // change to your org
version := "0.1.0"

scalaVersion := "2.13.8"

val CirceVersion  = "0.14.2"
val Http4sVersion = "0.22.0"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.11" % "test",

  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2" % Runtime,
  // https://mvnrepository.com/artifact/com.google.guava/guava/31.1-jre
  "com.google.guava" % "guava" % "31.1-jre"
)  

Compile / mainClass := Some("xyz.sourcecodestudy.spark.MainApp")

Compile / scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow", "-u", "target/junit-xml-reports", "-oD", "-eS")