package xyz.sourcecodestudy.spark.scheduler

class TaskSet(
    val tasks: Array[Task[_]],
    val stageId: Int,
    val attempt: Int,
    val priority: Int) {

  val id: String = s"${stageId}.${attempt}"

  def kill(interruptThread: Boolean): Unit = {
    tasks.foreach(_.kill(interruptThread))
  }

  override def toString: String = s"TaskSet ${id}"
}