package xyz.sourcecodestudy.spark.scheduler

import xyz.sourcecodestudy.spark.TaskContext

abstract class Task[T](val stageId: Int, val partitionId: Int) extends Serializable {

  //private   var taskThread: Thread = _
  protected var context: TaskContext = _

  final def run(attemptId: Long): T = {
    context = new TaskContext(stageId, partitionId, attemptId)
    //taskThread = Thread.currentThread()
    runTask(context)
  }

  def runTask(context: TaskContext): T

  def preferredLocations: Seq[TaskLocation] = Nil

}