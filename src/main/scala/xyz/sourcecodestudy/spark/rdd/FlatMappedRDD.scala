package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag
import scala.collection.IterableOnce

import xyz.sourcecodestudy.spark.{TaskContext, Partition}

class FlatMappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => IterableOnce[U]) extends RDD[U](prev) {
  
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = firstParent[T].iterator(split, context).flatMap(f)

}