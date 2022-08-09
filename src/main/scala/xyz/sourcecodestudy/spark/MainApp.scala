package xyz.sourcecodestudy.spark

import org.apache.logging.log4j.scala.Logging

object MainApp extends Logging {

  def main(args: Array[String]) = { 

    val sc = new SparkContext()
    logger.trace(s"Enter application, master = ${sc.master}")

    val rdd = sc.parallelize(Seq("aa", "A", "bb", "B", "cc", "C", "dd", "D", "X"), 3)

    rdd.foreachPartition(iter => println(s"${Thread.currentThread().getName}, ${iter.toSeq}"))

    rdd.reHashPartition(n => n.size, 2).map(e => e._2).foreachPartition((iter => println(s"${Thread.currentThread().getName}, ${iter.toSeq}")))
  }
}