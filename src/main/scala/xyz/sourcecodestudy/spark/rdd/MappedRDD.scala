package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.{TaskContext, Partition}

class MappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U) extends RDD[U](prev) {
  
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    logger.info(s"RddId ${id} MappedRDD.compute, dependencies = ${dependencies}")
    val ret = firstParent[T].iterator(split, context).map(f)
    logger.info(s"RrddId ${id}  End MappedRDD.compute， ret = ${ret}")
    ret
  }

}