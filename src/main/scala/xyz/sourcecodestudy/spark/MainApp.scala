package xyz.sourcecodestudy.spark

import org.apache.logging.log4j.scala.Logging

object MainApp extends Logging {

  def main(args: Array[String]) = { 

    val sc = new SparkContext()
    logger.trace(s"Enter application, master = ${sc.master}")

    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 3).map(n => n * 10).filter(n => n > 30).foreach(println _)
  }
}