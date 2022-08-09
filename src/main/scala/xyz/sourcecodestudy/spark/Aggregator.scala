package xyz.sourcecodestudy.spark

import xyz.sourcecodestudy.spark.util.collection.AppendOnlyMap

case class Aggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) {

  private val externalSorting = false

  def combineValueByKey(iter: Iterator[(K, V)], context: TaskContext): Iterator[(K, C)] = {
    if (!externalSorting) {
      val combiners = new AppendOnlyMap[K, C]
      var kv: (K, V) = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) {
          mergeValue(oldValue, kv._2)
        } else {
          createCombiner(kv._2)
        }
      }

      while (iter.hasNext) {
        kv = iter.next()
        combiners.changeValue(kv._1, update)
      }
      combiners.iterator
    } else {
      // TODO
      Seq[(K, C)]().iterator
    }
  }

  def combineCombinersByKey(iter: Iterator[(K, C)], context: TaskContext): Iterator[(K, C)] = {
    if (!externalSorting) {
      val combiners = new AppendOnlyMap[K, C]
      var kc: (K, C) = null

      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) {
          mergeCombiners(oldValue, kc._2)
        } else {
          kc._2
        }
      }

      while (iter.hasNext) {
        kc = iter.next()
        combiners.changeValue(kc._1, update)
      }

      combiners.iterator
    } else {
      // TODO
      Seq[(K, C)]().iterator
    }
  }
}