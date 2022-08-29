package xyz.sourcecodestudy.spark.scheduler

import java.nio.ByteBuffer
import xyz.sourcecodestudy.spark.TaskState.TaskState

trait SchedulerBackend {
  def start(): Unit
  def stop(): Unit
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  def killTask(taskId: Long, interruptThread: Boolean): Unit
    //= throw new UnsupportedOperationException

  // In ExecutorBackend, move to executor
  // def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit
}