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
  val numPartitions = rdd.partitions.size
  var numAvailableOutputs = 0
  private var nextAttemptId = 0

  def newAttemptId(): Int = {
    val id = nextAttemptId
    nextAttemptId += 1
    id
  }

  def isAvailable: Boolean = {
    if (!isShuffleMap) true
    else numAvailableOutputs == numPartitions
  }

  // TODO: shuffle

  override def toString(): String = s"Stage $id"

  override def hashCode(): Int = id
}