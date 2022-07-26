package spark

import java.util.{HashMap => JHashMap}

class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

class ShuffledRDD[K, V, C](
    parent: RDD[(K, V)],
    aggregator: Aggregator[K, V, C],
    part : Partitioner) 
  extends RDD[(K, C)](parent.context) {
  //override val partitioner = Some(part)
  override val partitioner = Some(part)
  
  @transient
  // Returns A traversable consisting of elements f(0),f(1), ..., f(n - 1)
  // 有点奇怪，这基本只是保存了有几个分区的信息，没有其他
  val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_
  
  override def preferredLocations(split: Split) = Nil
  
  // 新建的是Dependency，用的是shuffleId，注意和rddId区分开来
  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    def mergePair(k: K, c: C) {
      val oldC = combiners.get(k)
      if (oldC == null) {
        combiners.put(k, c)
      } else {
        combiners.put(k, aggregator.mergeCombiners(oldC, c))
      }
    }
    val fetcher = SparkEnv.get.shuffleFetcher
    // 取i个分区的数据，这里没有交代怎么存放数据的，需要进入fetch函数探索
    // mergePair里面使用了闭包变量combiners！！！
    fetcher.fetch[K, C](dep.shuffleId, split.index, mergePair)
    return new Iterator[(K, C)] {
      var iter = combiners.entrySet().iterator()

      def hasNext(): Boolean = iter.hasNext()

      def next(): (K, C) = {
        val entry = iter.next()
        (entry.getKey, entry.getValue)
      }
    }
  }
}
