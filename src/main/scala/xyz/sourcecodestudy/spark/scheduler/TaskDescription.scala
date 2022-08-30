package xyz.sourcecodestudy.spark.scheduler

import java.nio.ByteBuffer

class TaskDescription(
    val taskId: Long,
    val executorId: String,
    val name: String,
    val index: Int,
    _serializedTask: ByteBuffer) extends Serializable {
  
  def serializedTask: ByteBuffer = _serializedTask

  override def toString(): String = s"TaskDescription(taskId = ${taskId}, executorId = ${executorId}, index = ${index})"
}