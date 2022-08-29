package xyz.sourcecodestudy.spark.executor

import java.nio.ByteBuffer

import xyz.sourcecodestudy.spark.TaskState.TaskState

trait ExecutorBackend {

  def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit

}