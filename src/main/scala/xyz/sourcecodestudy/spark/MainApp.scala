package xyz.sourcecodestudy.spark

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.SparkContext._

// How to start driver:
// >sbt run

object MainApp extends Logging {

  def main(args: Array[String]) = {

    val sc = new SparkContext(new SparkConf(false).setAll(parseArguments(args)))

    logger.warn(s"Enter application, driver = ${sc.master}")
    
    val rdd0 = sc.parallelize(Seq("a", "aa", "aaa", "aaaa", "aaa", "aaa", "aa", "aaaa", "aaaa", "aaaa"), 3)

    rdd0.map(k => (k, 1.toLong)).count().foreach(p => println(s"count ${p._1} -> ${p._2}")) 

    val rdd1 = sc.parallelize(Seq("aa" -> 1, "bb" -> 2, "aa" -> 3, "bc" -> 4, "bc" -> 5, "cc" -> 6, "ac" -> 7, "ac" -> 8, "ab" -> 9), 3)

    rdd1.groupByKey(2).foreach(p => println(s"group ${p._1} -> ${p._2.toSeq}"))

    val rdd2 = sc.parallelize(Seq("aa" -> 10, "bb" -> 20, "aa" -> 30, "bc" -> 40, "bc" -> 50, "cc" -> 60, "ac" -> 70, "ac" -> 80, "ab" -> 90), 2)
  
    rdd2.cogroup(rdd1).foreach{ cg => println(s"k = ${cg._1}, ${cg._2._1.toSeq} | ${cg._2._2.toSeq}") }
    
    sc.stop()
  }

  private def parseArguments(args: Array[String]): Iterable[(String, String)] = {
    var configs = Seq[(String, String)]()
    var argv = args.toList

    while (!argv.isEmpty) {
      argv match {
        case ("--master") :: value :: tail =>
          configs = ("spark.master", value) +: configs
          argv = tail
        case ("--driver-port") :: value :: tail =>
          configs = ("spark.rpc.netty-port", value) +: configs
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          System.exit(-1)
      }
    }
    configs
  }
}