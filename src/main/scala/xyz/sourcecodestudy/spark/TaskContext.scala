package xyz.sourcecodestudy.spark

class TaskContext(
    val stageId: Int,
    val partitionId: Int,
    val attemptId: Long,
    val runningLocally: Boolean = false)
  extends Serializable