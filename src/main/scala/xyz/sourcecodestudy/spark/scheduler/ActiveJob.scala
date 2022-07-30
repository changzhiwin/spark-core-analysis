package xyz.sourcecodestudy.spark.scheduler

import org.apache.spark.TaskContext

class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,
    val func: (TaskContext, Iterator[_]) => _,
    val partitions: Array[Int]) {

  val numPartitions = partitions.length
  val finished = Array.fill[Boolean](numPartitions)(false)
  var numFinished = 0
}