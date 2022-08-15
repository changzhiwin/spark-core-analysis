name := "spark-core-analysis"
organization := "xyz.sourcecodestudy.spark" // change to your org
version := "0.1.0"

scalaVersion := "2.13.8"

val CirceVersion  = "0.14.2"
val Http4sVersion = "0.22.0"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.13" % "test",
  // "org.scalacheck" %% "scalacheck" % "1.15.4" % "test",

  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2" % Runtime,
  // https://mvnrepository.com/artifact/com.google.guava/guava/31.1-jre
  "com.google.guava" % "guava" % "31.1-jre",
  // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/ClosureCleaner.scala
  "org.apache.commons" % "commons-lang3" % "3.12.0",
  "org.apache.xbean" % "xbean-asm9-shaded" % "4.21",
  "commons-io" % "commons-io" % "2.11.0"
)  

Compile / mainClass := Some("xyz.sourcecodestudy.spark.MainApp")

Compile / scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow", "-u", "target/junit-xml-reports", "-oD", "-eS")