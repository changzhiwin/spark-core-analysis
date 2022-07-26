package xyz.sourcecodestudy.spark.scheduler

import xyz.sourcecodestudy.spark.TaskContext

class ResultTask[T, U](
    stageId: Int,
    val rdd: RDD[T],
    val func: () => U,
    _partitionId: Int,
    locs: Seq[TaskLocation],
    val outputId: Int) 
  extends Task[U](stageId, _partitionId) {

  override def runTask(context: TaskContext): U = {
    func(context, rdd.iterator(split, context))
  }  

  override def preferredLocations: Seq[TaskLocation] = if (locs == null) Nil else locs.toSet.toSeq

  override def toString(): String = s"ResultTask($stageId, $partitionId)"

}