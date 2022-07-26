package xyz.sourcecodestudy.spark.scheduler

class TaskLocation private (val host: String, val executorId: Option[String]) extends Serializable {
  override def toString: String = s"TaskLocation($host, $executorId)"
}

object TaskLocation {
  def apply(host: String, executorId: String) = new TaskLocation(host, Some(executorId))

  def apply(host: String) = new TaskLocation(host, None)
}