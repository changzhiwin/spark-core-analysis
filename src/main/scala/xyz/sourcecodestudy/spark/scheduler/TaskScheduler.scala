package xyz.sourcecodestudy.spark.scheduler

trait TaskScheduler {

  def start(): Unit

  def stop(): Unit

  def submitTasks(taskSet: TaskSet): Unit

  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  def defaultParallelism(): Int
}