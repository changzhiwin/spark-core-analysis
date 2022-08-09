package xyz.sourcecodestudy.spark

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.Aggregator
import xyz.sourcecodestudy.spark.util.collection.AppendOnlyMap

object MainApp extends Logging {

  def main(args: Array[String]) = {
    testAppendOnlyMap()
  }

  def testRePartition(): Unit = { 

    val sc = new SparkContext()
    logger.trace(s"Enter application, master = ${sc.master}")

    val rdd = sc.parallelize(Seq("aa", "A", "bb", "B", "cc", "C", "dd", "D", "X"), 3)

    rdd.foreachPartition(iter => println(s"${Thread.currentThread().getName}, ${iter.toSeq}"))

    rdd.reHashPartition(n => n.size, 2).map(e => e._2).foreachPartition((iter => println(s"${Thread.currentThread().getName}, ${iter.toSeq}")))
  }

  def testAppendOnlyMap(): Unit = {
    val map = new AppendOnlyMap[Int, Char]()
    var ret: Seq[(Int, Char)] = Nil

    for( (ch: Char, idx: Int) <- ('A' to 'Y').zipWithIndex) {
      ret = (idx, ch) +: ret
      map.update(idx, ch)
    }
    
    println(map.iterator.toArray.sortBy(t => t._1).toSeq == ret.sortBy(t => t._1).toSeq)
  }
}