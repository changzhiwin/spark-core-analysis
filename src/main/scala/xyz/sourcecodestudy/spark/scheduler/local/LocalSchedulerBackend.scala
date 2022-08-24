package xyz.sourcecodestudy.spark.scheduler.local

import java.nio.ByteBuffer

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.{SparkEnv, TaskState} // object
import xyz.sourcecodestudy.spark.TaskState.TaskState   // type
import xyz.sourcecodestudy.spark.executor.Executor
import xyz.sourcecodestudy.spark.scheduler.{TaskSchedulerImpl, SchedulerBackend, WorkerOffer}
import xyz.sourcecodestudy.spark.rpc.{RpcEnv, RpcEndpointRef, ThreadSafeRpcEndpoint, RpcCallContext}

private case class ReviveOffers()

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private case class KillTask(taskId: Long, interruptThread: Boolean, reason: String)

private case class StopExecutor()

class LocalEndpoint(
    override val rpcEnv: RpcEnv,
    scheduler: TaskSchedulerImpl,
    executorBackend: LocalSchedulerBackend,
    val totalCores: Int) extends ThreadSafeRpcEndpoint with Logging {

  private var freeCores = totalCores
  private val localExecutorId = "localhost-id"
  private val localExecutorHostname = "localhost-name"

  private val executor = new Executor(localExecutorId, true)
  override def onStart(): Unit = {
    logger.warn("start local endpoint.")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers => 
      reviveOffers()
    case StatusUpdate(taskId, state, data) =>
      scheduler.statusUpdate(taskId, state, data)
      if (TaskState.isFinished(state)) {
        freeCores += 1
        reviveOffers()
      }
    case KillTask(taskId, interruptThread, _) =>
      executor.killTask(taskId, interruptThread)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor => {
      executor.stop()
      context.reply(true)
    }
  }

  def reviveOffers(): Unit = {
    val offers = Seq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    for (task <- scheduler.resourceOffers(offers).flatten) {
      freeCores -= 1
      executor.launchTask(executorBackend, task.taskId, task.serializedTask)
    }
  }
}

class LocalSchedulerBackend(
    //conf: SparkConf,
    scheduler: TaskSchedulerImpl,
    val totalCores: Int) extends SchedulerBackend {

  private var localEndpointRef: RpcEndpointRef = null

  override def start(): Unit = {
    val rpcEnv = SparkEnv.get.rpcEnv
    val localEndpoint = new LocalEndpoint(rpcEnv, scheduler, this, totalCores)
    localEndpointRef = rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint", localEndpoint)
  }

  override def stop(): Unit = {
    localEndpointRef.ask(StopExecutor)
  }

  override def reviveOffers(): Unit = {
    localEndpointRef.send(ReviveOffers)
  }

  override def killTask(taskId: Long, interruptThread: Boolean): Unit = {
    localEndpointRef.send(KillTask(taskId, interruptThread, "Be Killed"))
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = {
    localEndpointRef.send(StatusUpdate(taskId, state, data))
  }

  override def defaultParallelism(): Int = totalCores
}