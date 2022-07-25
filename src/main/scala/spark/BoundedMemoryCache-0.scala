package spark

import java.util.LinkedHashMap

/**
 * An implementation of Cache that estimates the sizes of its entries and attempts to limit its
 * total memory usage to a fraction of the JVM heap. Objects' sizes are estimated using
 * SizeEstimator, which has limitations; most notably, we will overestimate total memory used if
 * some cache entries have pointers to a shared object. Nonetheless, this Cache should work well
 * when most of the space is used by arrays of primitives or of simple classes.
 */
class BoundedMemoryCache(maxBytes: Long) extends Cache with Logging {
  logInfo("BoundedMemoryCache.maxBytes = " + maxBytes)

  // 不指定最大值，就是用系统设置的计算
  def this() {
    this(BoundedMemoryCache.getMaxBytes)
  }

  private var currentBytes = 0L
  /**
    * LinkedHashMap(int capacity, float fillRatio, boolean Order)
    * This constructor is also used to initialize both the capacity and fill ratio 
    * for a LinkedHashMap along with whether to follow the insertion order or not.
    * 支持自动扩容
    */
  private val map = new LinkedHashMap[(Any, Int), Entry](32, 0.75f, true)

  override def get(datasetId: Any, partition: Int): Any = {
    synchronized {
      val entry = map.get((datasetId, partition))
      if (entry != null) {
        entry.value
      } else {
        null
      }
    }
  }

  override def put(datasetId: Any, partition: Int, value: Any): CachePutResponse = {
    val key = (datasetId, partition)
    logInfo("Asked to add key " + key)
    // 估算value占用的存储大小
    val size = estimateValueSize(key, value)
    synchronized {
      if (size > getCapacity) {
        return CachePutFailure()
      } else if (ensureFreeSpace(datasetId, size)) {
        logInfo("Adding key " + key)
        map.put(key, new Entry(value, size))
        currentBytes += size
        logInfo("Number of entries is now " + map.size)
        return CachePutSuccess(size)
      } else {
        logInfo("Didn't add key " + key + " because we would have evicted part of same dataset")
        return CachePutFailure()
      }
    }
  }

  override def getCapacity: Long = maxBytes

  /**
   * Estimate sizeOf 'value'
   */
  private def estimateValueSize(key: (Any, Int), value: Any) = {
    val startTime = System.currentTimeMillis
    val size = SizeEstimator.estimate(value.asInstanceOf[AnyRef])
    val timeTaken = System.currentTimeMillis - startTime
    logInfo("Estimated size for key %s is %d".format(key, size))
    logInfo("Size estimation for key %s took %d ms".format(key, timeTaken))
    size
  }

  /**
   * Remove least recently used entries from the map until at least space bytes are free, in order
   * to make space for a partition from the given dataset ID. If this cannot be done without
   * evicting other data from the same dataset, returns false; otherwise, returns true. Assumes
   * that a lock is held on the BoundedMemoryCache.
   * 确认是否有足够的内存，来缓存这个rdd
   */
  private def ensureFreeSpace(datasetId: Any, space: Long): Boolean = {
    logInfo("ensureFreeSpace(%s, %d) called with curBytes=%d, maxBytes=%d".format(
      datasetId, space, currentBytes, maxBytes))

    // 下面使用了缓存的经典算法：LRU，注意创建LinkedHashMap时的最后一个参数：Order此时发挥作用了
    val iter = map.entrySet.iterator   // Will give entries in LRU order
    while (maxBytes - currentBytes < space && iter.hasNext) {
      val mapEntry = iter.next()
      // 注意这个key的组成：rddId + partition，缓存的单元不是rdd，而是partition
      val (entryDatasetId, entryPartition) = mapEntry.getKey
      if (entryDatasetId == datasetId) {
        // Cannot make space without removing part of the same dataset, or a more recently used one
        return false
      }
      reportEntryDropped(entryDatasetId, entryPartition, mapEntry.getValue)
      // 拆东墙，补西墙
      currentBytes -= mapEntry.getValue.size
      iter.remove()
    }
    return true
  }

  protected def reportEntryDropped(datasetId: Any, partition: Int, entry: Entry) {
    logInfo("Dropping key (%s, %d) of size %d to make space".format(datasetId, partition, entry.size))
    SparkEnv.get.cacheTracker.dropEntry(datasetId, partition)
  }
}

// An entry in our map; stores a cached object and its size in bytes
case class Entry(value: Any, size: Long)

object BoundedMemoryCache {
  /**
   * Get maximum cache capacity from system configuration
   */
   def getMaxBytes: Long = {
    /**
      * 为了理解maxMemory，参考了：
      * https://stackoverflow.com/questions/3571203/what-are-runtime-getruntime-totalmemory-and-freememory
      * https://stackoverflow.com/questions/23701207/why-do-xmx-and-runtime-maxmemory-not-agree
      * https://blog.csdn.net/wisgood/article/details/79850093
      * https://blog.csdn.net/wgw335363240/article/details/8878644
      * 我的理解：
      * java -Xms64m -Xmx1024m FooApp
      * 参数-Xmx，指定堆内存的申请上限
      * 参数-Xms，指定启动的时候就要立即申请到的堆内存大小，不指定时，边用边申请，直到-Xmx
      * maxMemory基本上是和-Xmx相等的（Eden + 2*Survivor + Tenured），有一个Survivor没有算
      * totalMemory已经申请的总和
      * freeMemory已经申请，但没有被使用的
      */
    val memoryFractionToUse = System.getProperty("spark.boundedMemoryCache.memoryFraction", "0.66").toDouble
    (Runtime.getRuntime.maxMemory * memoryFractionToUse).toLong
  }
}

