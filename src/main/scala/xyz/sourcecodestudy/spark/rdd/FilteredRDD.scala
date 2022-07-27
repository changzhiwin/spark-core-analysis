package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.{TaskContext, Partition}

class FilteredRDD[T: ClassTag](prev: RDD[T], f: T => Boolean) extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = prev.partitioner

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T].iterator(split, context).filter(f)
  }

}