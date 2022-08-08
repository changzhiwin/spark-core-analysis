package xyz.sourcecodestudy.spark

import xyz.sourcecodestudy.spark.rdd.RDD
import xyz.sourcecodestudy.spark.serializer.Serializer

abstract class Dependency[T](val rdd: RDD[T]) extends Serializable

abstract class NarrowDependency[T](rdd: RDD[T]) extends Dependency[T](rdd) {

  def getParents(partitionId: Int): Seq[Int]

}

// Upper type bounds specify that a type must be a subtype of another type
class ShuffleDependency[K, V](
    rdd: RDD[(K, V)],
    val partitioner: Partitioner,
    val serializer: Serializer = null)
  //extends Dependency(rdd) {
  extends Dependency(rdd) {

  val shuffleId: Int = rdd.context.newShuffledId()

}

class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  
  override def getParents(partitionId: Int): Seq[Int] = List(partitionId)

}

class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int) extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): Seq[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }

}