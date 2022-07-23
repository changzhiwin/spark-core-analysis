package spark

import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.util.{HashMap => JHashMap}

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

class ShuffleMapTask(
    runId: Int,
    stageId: Int,
    rdd: RDD[_], 
    dep: ShuffleDependency[_,_,_],
    val partition: Int, 
    locs: Seq[String])
  extends DAGTask[String](runId, stageId)
  with Logging {
  
  // 取在哪一个分区
  val split = rdd.splits(partition)

  override def run (attemptId: Int): String = {
    val numOutputSplits = dep.partitioner.numPartitions
    val aggregator = dep.aggregator.asInstanceOf[Aggregator[Any, Any, Any]]
    val partitioner = dep.partitioner.asInstanceOf[Partitioner]

    // 每个分区一个Hash Map来缓存
    /**
      * 0                 JHashMap[Any, Any]
      * 1                 JHashMap[Any, Any]
      * 2                 JHashMap[Any, Any]
      * ...               JHashMap[Any, Any]
      * numOutputSplits   JHashMap[Any, Any]
      */
    val buckets = Array.tabulate(numOutputSplits)(_ => new JHashMap[Any, Any])

    // 依次遍历该分区，rdd.iterator会调用rdd.compute
    for (elem <- rdd.iterator(split)) {
      val (k, v) = elem.asInstanceOf[(Any, Any)]
      // 应该去往那个分区
      var bucketId = partitioner.getPartition(k)
      // 
      val bucket = buckets(bucketId)
      var existing = bucket.get(k)
      if (existing == null) {
        // 第一个元素，create
        bucket.put(k, aggregator.createCombiner(v))
      } else {
        // 之后都是merge
        bucket.put(k, aggregator.mergeValue(existing, v))
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    // 把数据写入numOutputSplits个文件，这个值是ShuffleDependency决定的！！！
    for (i <- 0 until numOutputSplits) {
      //仔细理解：partition是被shuffle的rdd的第几个分区；i是这个分区被partitioner切分成的一块
      //因为当前rdd的每个分区，都需要shuffle成numOutputSplits块，为后续的shuffle read做准备
      val file = SparkEnv.get.shuffleManager.getOutputFile(dep.shuffleId, partition, i)
      val out = ser.outputStream(new FastBufferedOutputStream(new FileOutputStream(file)))
      // 先写入总个数
      out.writeObject(buckets(i).size)
      // 然后依次写入每个数据
      val iter = buckets(i).entrySet().iterator()
      while (iter.hasNext()) {
        val entry = iter.next()
        out.writeObject((entry.getKey, entry.getValue))
      }
      // TODO: have some kind of EOF marker
      out.close()
    }
    return SparkEnv.get.shuffleManager.getServerUri
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}
