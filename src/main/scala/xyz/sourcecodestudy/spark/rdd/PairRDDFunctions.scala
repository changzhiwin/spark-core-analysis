package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.SparkContext._
import xyz.sourcecodestudy.spark.{Partitioner, HashPartitioner, Aggregator}
import xyz.sourcecodestudy.spark.serializer.Serializer

class PairRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)])(implicit ord: Ordering[K]) extends Logging with Serializable {

  def partitionBy(partitioner: Partitioner): RDD[(K, V)] = {
    new ShuffledRDD[K, V, (K, V)](self, partitioner)
  }

  // 实现count
  def count(): RDD[(K, Long)] = {
    // 定义Aggregator参数
    def createCombiner(v: V) = v.asInstanceOf[Long]
    def mergeValue(buf: Long, v: V) = buf + v.asInstanceOf[Long]
    def mergeCombiners(c1: Long, c2: Long) = c1 + c2

    combineByKey[Long](
      createCombiner,
      mergeValue,
      mergeCombiners,
      new HashPartitioner(self.partitions.size),
      true,
      null
    )
  }

  def groupByKey(partitioner: Partitioner): RDD[(K, Iterator[V])] = {
    // 定义Aggregator参数
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(c1: ArrayBuffer[V], c2: ArrayBuffer[V]) = c1 ++ c2

    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner,
      mergeValue,
      mergeCombiners,
      partitioner,
      false,
      null
    )

    bufs.mapValues(_.iterator)
  }

  def groupByKey(numPartitions: Int): RDD[(K, Iterator[V])] = {
    groupByKey(new HashPartitioner(numPartitions))
  }
  
  // 这个是最基础的方法
  def combineByKey[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null): RDD[(K, C)] = {

    val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)
    if (self.partitioner == Some(partitioner)) {

      self.mapPartitionsWithContext((context, iter) => {
        aggregator.combineValuesByKey(iter, context)
      }, preservesPartitioning = true)

    } else {

      mapSideCombine match {
        case true  => {
          // 1, map端combine
          val combined = self.mapPartitionsWithContext((context, iter) => {
            aggregator.combineValuesByKey(iter, context)
          }, preservesPartitioning = true)

          // 2，需要shuffle
          val partitioned = new ShuffledRDD[K, C, (K, C)](combined, partitioner).setSerializer(serializer)

          // 3, reduce端的combine
          partitioned.mapPartitionsWithContext((context, iter) => {
            aggregator.combineCombinersByKey(iter, context)
          }, preservesPartitioning = true)
        }
          
        case false => {
          // map端不需要合并，调过1，进行2
          val values = new ShuffledRDD[K, V, (K, V)](self, partitioner).setSerializer(serializer)

          // 3, reduce端的combine
          values.mapPartitionsWithContext((context, iter) => {
            aggregator.combineValuesByKey(iter, context)
          }, preservesPartitioning = true)
        }

      } // end match
    }
  }

  def mapValues[U: ClassTag](f: V => U): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MappedValuesRDD[K, V, U](self, cleanF) //
  }

  // 元方法，join基于该方法构建
  def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W]))] = {
    val cg = new CoGroupedRDD[K](Seq(self.asInstanceOf[RDD[(K, _)]], other.asInstanceOf[RDD[(K, _)]]), partitioner)

    cg.mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
    }
  }

  def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = {
    val numPartitions = if (self.partitions.size > other.partitions.size) self.partitions.size else other.partitions.size
    cogroup(other, new HashPartitioner(numPartitions))
  }
}