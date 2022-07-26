package xyz.sourcecodestudy.spark.scheduler

import xyz.sourcecodestudy.spark.TaskContext

class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,
    val func: (TaskContext, Iterator[_]) => _,
    val partitions: Array[Int],
    val listener: JobListener) {

  val numPartitions = partitions.length
  val finished = Array.fill[Boolean](numPartitions)(false)
  var numFinished = 0
}