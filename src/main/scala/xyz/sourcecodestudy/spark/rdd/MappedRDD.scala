package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.{TaskContext, Partition}

class MappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U) extends RDD[U](prev) {
  
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    firstParent[T].iterator(split, context).map(f)
  }

}