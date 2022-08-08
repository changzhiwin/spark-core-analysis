package xyz.sourcecodestudy.spark.scheduler

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.ShuffleDependency
import xyz.sourcecodestudy.spark.rdd.RDD

class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val shuffleDep: Option[ShuffleDependency[_,_]],  // Output shuffle if stage is a map stage
    val parents: List[Stage],
    val jobId: Int) extends Logging {

  val isShuffleMap = shuffleDep.isDefined
  val numPartitions: Int = rdd.partitions.size
  var numAvailableOutputs = 0
  private var nextAttemptId = 0

  /**
    * +-------------------------------------+
    * |  Partition    |    Shuffle List     |
    * +-------------------------------------+
    * |  partition 1  |  List(hash1, hash2) |
    * +-------------------------------------+
    * |  partition 2  |  List(hash1, hash2) |
    * +-------------------------------------+
    * |  partition 3  |  List(hash1, hash2) |
    * +-------------------------------------+
    */
  val outputLocs = Array.fill[List[String]](numPartitions)(Nil)

  def addOutputLoc(partition: Int, address: String): Unit = {
    val prevList = outputLocs(partition)
    outputLocs(partition) = address :: prevList
    // 只有一个分区首次加入数据，才计数
    if (prevList == Nil) {
      numAvailableOutputs += 1
    }
  }

  def removeOutputLoc(partition: Int, address: String): Unit = {
    val prevList = outputLocs(partition)
    val newList =  prevList.filterNot(_ == address)
    outputLocs(partition) = newList
    // 只有一个分区所有数据都移除，才计数
    if (prevList != Nil && newList == Nil) {
      numAvailableOutputs -= 1
    }
  }

  def newAttemptId(): Int = {
    val id = nextAttemptId
    nextAttemptId += 1
    id
  }

  def isAvailable: Boolean = {
    if (!isShuffleMap) true
    else numAvailableOutputs == numPartitions
  }

  override def toString(): String = s"Stage $id"

  override def hashCode(): Int = id
}