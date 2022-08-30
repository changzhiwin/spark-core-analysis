package xyz.sourcecodestudy.spark.scheduler.cluster

import java.nio.ByteBuffer

import xyz.sourcecodestudy.spark.TaskState.TaskState
import xyz.sourcecodestudy.spark.rpc.{RpcEndpointRef}

sealed trait CoarseGrainedClusterMessage extends Serializable

object CoarseGrainedClusterMessage {

  case class LaunchTask(taskId: Long, data: ByteBuffer) extends CoarseGrainedClusterMessage

  case class KillTask(
      taskId: Long, 
      executor: String, 
      interruptThread: Boolean) extends CoarseGrainedClusterMessage

  case class RegisterExecutor(
      executorId: String,
      executorRef: RpcEndpointRef,
      hostname: String,
      cores: Int) extends CoarseGrainedClusterMessage

  case object RegisteredExecutor extends CoarseGrainedClusterMessage

  case class LaunchedExecutor(executorId: String) extends CoarseGrainedClusterMessage

  case class StatusUpdate(
      executorId: String,
      taskId: Long,
      state: TaskState,
      // data: SerializableBuffer, // do not know why
      data: ByteBuffer) extends CoarseGrainedClusterMessage

  case object ReviveOffers extends CoarseGrainedClusterMessage

  case object StopDriver extends CoarseGrainedClusterMessage

  case object StopExecutor extends CoarseGrainedClusterMessage

  //case object StopExecutors extends CoarseGrainedClusterMessage

  //case class RemoveExecutor(executorId: String, reason: String) extends CoarseGrainedClusterMessage

  //case class RemoveWorker(workerId: String, host: String, message: String) extends CoarseGrainedClusterMessage

  //case class SetupDriver(driver: RpcEndpointRef) extends CoarseGrainedClusterMessage

  //case class KillExecutors(executorId: Seq[String]) extends CoarseGrainedClusterMessage

  case object ShutDown extends CoarseGrainedClusterMessage

}