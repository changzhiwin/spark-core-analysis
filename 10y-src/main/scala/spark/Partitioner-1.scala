package spark

abstract class Partitioner extends Serializable {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

class HashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = {
    if (key == null) {
      return 0
    } else {
      val mod = key.hashCode % partitions
      if (mod < 0) {
        mod + partitions
      } else {
        mod // Guard against negative hash codes
      }
    }
  }
  
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}

class RangePartitioner[K <% Ordered[K]: ClassManifest, V](
    partitions: Int,
    @transient rdd: RDD[(K,V)],
    private val ascending: Boolean = true) 
  extends Partitioner {

  // An array of upper bounds for the first (partitions - 1) partitions
  private val rangeBounds: Array[K] = {
    if (partitions == 1) {
      Array()
    } else {
      val rddSize = rdd.count()
      val maxSampleSize = partitions * 20.0
      val frac = math.min(maxSampleSize / math.max(rddSize, 1), 1.0)
      val rddSample = rdd.sample(true, frac, 1).map(_._1).collect().sortWith(_ < _)
      if (rddSample.length == 0) {
        Array()
      } else {
        val bounds = new Array[K](partitions - 1)
        for (i <- 0 until partitions - 1) {
          val index = (rddSample.length - 1) * (i + 1) / partitions
          bounds(i) = rddSample(index)
        }
        bounds
      }
    }
  }

  def numPartitions = partitions

  def getPartition(key: Any): Int = {
    // TODO: Use a binary search here if number of partitions is large
    val k = key.asInstanceOf[K]
    var partition = 0
    while (partition < rangeBounds.length && k > rangeBounds(partition)) {
      partition += 1
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_,_] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }
}

