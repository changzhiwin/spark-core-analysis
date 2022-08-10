package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.{TaskContext, Partition}

class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false) extends RDD[U](prev) {
  
  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override protected def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = 
    f(context, split.index, firstParent[T].iterator(split, context))
}