package xyz.sourcecodestudy.spark

import xyz.sourcecodestudy.spark.rdd.RDD

abstract class Dependency[T](val rdd: RDD[T]) extends Serializable

abstract class NarrowDependency[T](rdd: RDD[T]) extends RDD[T] {

  def getParents(partitionId: Int): Seq[Int]

}

// Upper type bounds specify that a type must be a subtype of another type
class ShuffleDependency[K, V](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    serializer: Serializer = null)
  extends Dependency(rdd.asInstanceOf[RDD[Product2[K, V]]]) {

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