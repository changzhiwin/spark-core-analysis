package xyz.sourcecodestudy.spark.scheduler

import xyz.sourcecodestudy.spark.rdd.RDD
import xyz.sourcecodestudy.spark.TaskContext

class ResultTask[T, U](
    stageId: Int,
    val rdd: RDD[T],
    val func: (TaskContext, Iterator[T]) => U,
    _partitionId: Int,
    //locs: Seq[TaskLocation],
    val outputId: Int) 
  extends Task[U](stageId, _partitionId) {

  val split = if (rdd == null) null else rdd.partitions(partitionId)

  override def runTask(context: TaskContext): U = {
    try {
      func(context, rdd.iterator(split, context))
    } finally {
      //TODO, context do something, like callback
    }
    
  }  

  @transient override def preferredLocations: Seq[TaskLocation] = Nil // if (locs == null) Nil else locs.toSet.toSeq

  override def toString(): String = s"ResultTask($stageId, $partitionId)"

}