package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.{TaskContext, Partition}

class MappedValuesRDD[K: ClassTag, V: ClassTag, U: ClassTag](prev: RDD[(K, V)], f: V => U) extends RDD[(K, U)](prev) {
  
  override def getPartitions: Array[Partition] = firstParent[(K, V)].partitions

  override val partitioner = firstParent[(K, V)].partitioner

  override def compute(split: Partition, context: TaskContext): Iterator[(K, U)] = {
    firstParent[(K, V)].iterator(split, context).map {
      case (k: K, v: V) => (k, f(v))
    }
  }

}