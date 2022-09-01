package xyz.sourcecodestudy.spark.scheduler.cluster

//import java.util.concurrent.TimeUnit

import scala.collection.mutable.{HashMap}

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.{TaskState, SparkException} //
import xyz.sourcecodestudy.spark.rpc.{RpcAddress, RpcEnv, IsolatedRpcEndpoint, RpcCallContext}
import xyz.sourcecodestudy.spark.scheduler.{TaskSchedulerImpl, SchedulerBackend, WorkerOffer, TaskDescription}
import xyz.sourcecodestudy.spark.util.{ThreadUtils, Utils, SerializableBuffer}

class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val rpcEnv: RpcEnv) 
    extends SchedulerBackend with Logging {

  import CoarseGrainedClusterMessage._

  protected val conf = scheduler.sc.conf

  protected var currentExecutorIdCounter = 0
  
  private val executorDataMap = new HashMap[String, ExecutorData]

  private val reviveThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-revive-thread")

  class DriverEndpoint extends IsolatedRpcEndpoint with Logging {

    override val rpcEnv: RpcEnv = CoarseGrainedSchedulerBackend.this.rpcEnv

    protected val addressToExecutorId = new HashMap[RpcAddress, String]

    override def onStart(): Unit = {
      // 定时触发 ReviveOffers 任务
      // 可以不启用，因为并发小，而且其他事件也可以触发，如LaunchedExecutor
      /*
      val reviveIntervalMs = 30000L // get conf TODO
      reviveThread.scheduleAtFixedRate(
        () => Utils.tryLogNonFatalError {
          Option(self).foreach(_.send(ReviveOffers))
        },
        0,
        reviveIntervalMs,
        java.util.concurrent.TimeUnit.MILLISECONDS
      )
      */
    }

    override def receive: PartialFunction[Any, Unit] = {
      case StatusUpdate(executorId, taskId, state, data) =>
        // 取value，保持scheduler的接口可不变
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          executorDataMap.get(executorId) match {
            case Some(executorInfo) =>
              executorInfo.freeCores += 1 // default taskCpus = 1
              makeOffers(executorId)
            case None =>
              logger.warn(s"Ignored task status update (${taskId} state ${state}) for unknow executorId(${executorId})")
          }
        }

      case ReviveOffers =>
        makeOffers()

      case KillTask(taskId, executorId, interruptThread) =>
        executorDataMap.get(executorId) match {
          case Some(executorInfo) =>
            executorInfo.executorEndpoint.send(
              KillTask(taskId, executorId, interruptThread)
            )
          case None => {
            logger.warn(s"Attempted to kill task(${taskId}) for unknow executorId(${executorId})")
          }
        }

      case LaunchedExecutor(executorId) =>
        executorDataMap.get(executorId).foreach { info =>
          info.freeCores = info.totalCores
        }
        makeOffers(executorId)

      case e =>
        logger.error(s"Received unexpected message. ${e}")
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterExecutor(executorId, executorRef, hostname, cores) =>
        if (executorDataMap.contains(executorId)) {
          context.sendFailure(new IllegalStateException(s"Duplicate executor ID: ${executorId}"))
        } else {
          val executorAddress = executorRef.address match {
            case Some(refAddr) => refAddr
            case None      => context.senderAddress
          }

          logger.info(s"Registered executor ${executorRef} ($executorAddress) with ID ${executorId}")

          addressToExecutorId(executorAddress) = executorId

          val data = new ExecutorData(executorRef, executorAddress, hostname, 0, cores, registrationTs = System.currentTimeMillis())

          CoarseGrainedSchedulerBackend.this.synchronized {
            executorDataMap.put(executorId, data)
            if (currentExecutorIdCounter < executorId.toInt) {
              currentExecutorIdCounter = executorId.toInt
            }
          }

          context.reply(true)
        }

      case RquestMapOut(shuffleId, reduceId) =>
        val mapOutTracker = scheduler.sc.env.mapOutputTracker
        if (mapOutTracker.containsShuffle(shuffleId)) {
          val status = mapOutTracker.getServerStatuses(shuffleId, reduceId)
          logger.info(s"Get mapOut of shuffleId =  ${shuffleId}: ${status.mkString(",")}")
          context.reply(ResponseMapOut(status.toSeq))
        } else{
          context.reply(ResponseMapOut(Seq.empty[String]))
          logger.warn(s"Driver have not shuffleId = ${shuffleId}")
        }

      case StopDriver =>
        context.reply(true)
        logger.warn(s"Received ask: StopDriver")

      case e => 
        logger.error(s"Received unexpected ask ${e}")
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId.get(remoteAddress).foreach { executorId =>
        removeExecutor(executorId)
      }
    }

    private def makeOffers(): Unit = {
      val taskDescs = withLock {
        val activeExecutors = executorDataMap

        val workOffers = activeExecutors.map {
          case (id, executorData) =>
            new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
        }.toIndexedSeq

        scheduler.resourceOffers(workOffers/*, true*/)
      }

      if (taskDescs.nonEmpty) {
        launchTasks(taskDescs)
      }
    }

    private def makeOffers(executorId: String): Unit = {

      val taskDescs = withLock {
        if (executorDataMap.contains(executorId)) {
          val executorData = executorDataMap(executorId)
          val workOffers = IndexedSeq(
            new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores)
          )
          scheduler.resourceOffers(workOffers)
        } else {
          Seq.empty
        }
      }

      if (taskDescs.nonEmpty) {
        launchTasks(taskDescs)
      }
    }

    private def launchTasks(tasks: Seq[Seq[TaskDescription]]): Unit = {
      for (task <- tasks.flatten) {   
        logger.info(s"launchTasks executorId = ${task.executorId}, taskId = ${task.taskId}")  
        // ignore rpc message size
        val executorData = executorDataMap(task.executorId)
        executorData.freeCores -= 1
        
        executorData.executorEndpoint.send(LaunchTask(task.taskId, SerializableBuffer(task.serializedTask)))
      }
    }

    private def removeExecutor(executorId: String): Unit = {

      logger.warn(s"Asked to remove executor ${executorId}.")

      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          CoarseGrainedSchedulerBackend.this.synchronized {
            addressToExecutorId -= executorInfo.executorAddress
            executorDataMap -= executorId
          }
        case None =>
          logger.warn(s"Asked to remove no-existent executor ${executorId}")
      }
    }

  } // end class DriverEndpoint

  val driverEndpoint = rpcEnv.setupEndpoint(
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME, 
    createDriverEndpoint()
  )

  protected def createDriverEndpoint(): DriverEndpoint = new DriverEndpoint()

  override def start(): Unit = {
    logger.info("Start CoarseGrainedSchedulerBackend.")
  }

  override def stop(): Unit = {
    reviveThread.shutdownNow()
    try {
      Option(driverEndpoint).foreach { ref =>
        ref.askSync[Boolean](StopDriver)
      }
    } catch {
      case e: Throwable =>
        throw new SparkException("Stop CoarseGrainedSchedulerBackend failed.", e)
    }
  }

  override def reviveOffers(): Unit = Utils.tryLogNonFatalError {
    driverEndpoint.send(ReviveOffers)
  }

  override def killTask(
    taskId: Long,
    executorId: String,
    interruptThread: Boolean,
    reason: String
  ): Unit = {
    driverEndpoint.send(KillTask(taskId, executorId, interruptThread))
  }

  override def defaultParallelism(): Int = {
    conf.get("spark.default.parallelism", "2").toInt
  }

  private def withLock[T](fn: => T): T = scheduler.synchronized {
    CoarseGrainedSchedulerBackend.this.synchronized { fn }
  }
}

object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}