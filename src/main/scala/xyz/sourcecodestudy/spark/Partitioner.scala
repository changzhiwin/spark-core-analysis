package xyz.sourcecodestudy.spark

// import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.rdd.RDD
import xyz.sourcecodestudy.spark.util.Utils

abstract class Partitioner extends Serializable {

  def numPartitions: Int

  def getPartition(key: Any): Int

}

object Partitioner {
  
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.size).reverse

    // Seq find the first one, return Option[A]
    bySize.find(_.partitioner.isDefined) match {
      case Some(r) => r.partitioner.get
      case None    => new HashPartitioner(bySize.head.partitions.size)
    }
  }
}

class HashPartitioner(partitions: Int) extends Partitioner {

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _    => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner => h.numPartitions == numPartitions
    case _                  => false
  }

}