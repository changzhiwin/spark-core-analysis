package xyz.sourcecodestudy.spark

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.SparkContext._

object MainApp extends Logging {

  def main(args: Array[String]) = {

    val sc = new SparkContext()
    logger.trace(s"Enter application, master = ${sc.master}")

    val rdd0 = sc.parallelize(Seq("a", "aa", "aaa", "aaaa", "aaa", "aaa", "aa", "aaaa", "aaaa", "aaaa"), 3)

    //.foreachPartition(p => println(s"part ${p.toSeq}"))
    rdd0.map(k => (k, 1.toLong)).count().foreach(p => println(s"count ${p._1} -> ${p._2}")) 

    val rdd1 = sc.parallelize(Seq("aa" -> 1, "bb" -> 2, "aa" -> 3, "bc" -> 4, "bc" -> 5, "cc" -> 6, "ac" -> 7, "ac" -> 8, "ab" -> 9), 3)

    rdd1.groupByKey(2).foreach(p => println(s"group ${p._1} -> ${p._2.toSeq}"))
  }

}