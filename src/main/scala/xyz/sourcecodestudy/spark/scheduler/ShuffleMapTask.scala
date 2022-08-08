package xyz.sourcecodestudy.spark.scheduler

import java.io.FileOutputStream

import scala.collection.mutable.{ArrayBuffer}

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.util.Utils
import xyz.sourcecodestudy.spark.serializer.Serializer
import xyz.sourcecodestudy.spark.rdd.RDD
import xyz.sourcecodestudy.spark.{TaskContext, Partitioner, ShuffleDependency}

class ShuffleMapTask(
    stageId: Int,
    val rdd: RDD[_],
    val dep: ShuffleDependency[_, _],
    _partitionId: Int,
    @transient private val locs: Seq[String]) extends Task[String](stageId, _partitionId) with Logging {

  val split = rdd.partitions(partitionId)
    
  override def runTask(context: TaskContext): String = {
    val numOutputSplits = dep.partitioner.numPartitions
    val partitioner = dep.partitioner.asInstanceOf[Partitioner]
    val ser = Serializer.getSerializer(dep.serializer).newInstance()

    val buckets = Array.tabulate(numOutputSplits)(_ => ArrayBuffer[(Any, Any)]())

    for (elem <- rdd.iterator(split, context)) {
      val (k, v) = elem.asInstanceOf[(Any, Any)]
      val bucketId = partitioner.getPartition(k)
      val bucket = buckets(bucketId)
      bucket += (k -> v)
    }

    // 写入每个分区的数据
    for (i <- 0 until numOutputSplits) {
      val file = Utils.getShuffledOutputFile(dep.shuffleId, split.index, i)
      logger.info(s"Shuffle Write: elem [${buckets(i).size}], file [${file}]")

      val out = ser.serializeStream(new FileOutputStream(file))

      try {
        out.writeObject(buckets(i).size)
        buckets(i).foreach {
          case (k, v) => out.writeObject( (k -> v) )
        }
      } finally {
        out.close()
      }
    }

    s"local-shuffle-${dep.shuffleId}-part-${_partitionId}"
  }

  override def toString = s"ShuffleMapTask(${stageId}, ${partitionId})"
}