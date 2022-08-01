package xyz.sourcecodestudy.spark.scheduler.local

import java.nio.ByteBuffer

import xyz.sourcecodestudy.spark.TaskState.TaskState
import xyz.sourcecodestudy.spark.executor.Executor
import xyz.sourcecodestudy.spark.scheduler.SchedulerBackend

class LocalBackend(scheduler: TaskSchedulerImpl, val totalCores: Int)
  extends SchedulerBackend {

  private var freeCores = totalCores
  private val localExecutorId = "localhost-id"
  private val localExecutorHostname = "localhost-name"

  val executor = new Executor(localExecutorId, true)
  
  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def reviveOffers(): Unit = {

    val offers = Seq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    for (task <- scheduler.resourceOffers(offers).flatten) {
      freeCores -= 1
      executor.launchTask(this, task.taskId, task.serializedTask)
    }
  }

  override def defaultParallelism(): Int = totalCores

  override def killTask(taskId: Long, interruptThread: Boolean): Unit = {
    executor.killTask(taskId, interruptThread)
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = {
    scheduler.statusUpdate(taskId, state, data)
    if (TaskState.isFinished(state)) {
      freeCores += 1
      // 疑问：不清楚为啥这里调用
      // 回答：一个任务完成，释放了资源，可以申请再次执行了
      reviveOffers()
    }
  }
}