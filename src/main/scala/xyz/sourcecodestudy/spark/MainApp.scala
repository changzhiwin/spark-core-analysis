package xyz.sourcecodestudy.spark

import org.apache.logging.log4j.scala.Logging

object MainApp extends Logging {

  def main(args: Array[String]) = { 

    val sc = new SparkContext()
    logger.trace(s"Enter application, master = ${sc.master}")

    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)

    val result = rdd.map(n => n * 10).filter(n => n > 50).collect()

    logger.error(s"Result = ${result.toSeq}")

    val lens = sc.parallelize(Seq("spark", "foobar", "scala"), 3).map(n => n.length).collect()

    logger.error(s"lens = ${lens.toSeq}")
  }
}