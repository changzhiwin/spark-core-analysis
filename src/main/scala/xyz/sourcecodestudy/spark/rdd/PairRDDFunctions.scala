package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.{Partitioner}

class PairRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)])(implicit ord: Ordering[K]) extends Logging with Serializable {

  def partitionBy(partitioner: Partitioner): RDD[(K, V)] = {
    new ShuffledRDD[K, V, (K, V)](self, partitioner)
  }

  /*
  def groupByKey(partitioner: Partitioner): RDD[(K, V)] = { //RDD[(K, Iterator[V])] = {
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
  }
  */

  
  /*
    暂不实现Combine
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
  def combineByKey[C](
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null): RDD[(K, C)] = {

    if (self.partitioner == Some(partitioner)) {
      // TODO
      // 这种情况不需要shuffle
    } else {
      mapSideCombine match {
        case true  => 
          // TODO，这种情况需要map端的combine
        case false =>
          val values = new ShuffledRDD[K, V, (K, V)](self, partitioner)
      }
    }
  }
  */

  /*
  def mapValues[U](f: V => U): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MappedValuesRDD(self, cleanF)
  }
  */
}