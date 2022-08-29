package xyz.sourcecodestudy.spark.scheduler.cluster

import xyz.sourcecodestudy.spark.util.ThreadUtils
import java.util.concurrent.TimeUnit

class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val rpcEnv: RpcEnv) 
    extends SchedulerBackend with Logging {
  
  private val executorDataMap = new HashMap[String, ExecutorData]

  //private val executorPendingLossReason = new HashSet[String]

  private val reviveThread = ThreadUtils.newDaemonSingleThreadScheduledExecutro("driver-revive-thread")

  class DriverEndpoint extends IsolatedRpcEndpoint with Logging {

    override val rpcEnv: RpcEnv = CoarseGrainedSchedulerBackend.this.rpcEnv

    protected val addressToExecutorId = new HashMap[RpcAddress, String]

    override def onStart(): Unit = {
      val reviveIntervalMs = 1000L // get conf TODO

      reviveThread.scheduleAtFixedRate(
        () => Utils.tryLogNonFatalError {
          Option(self).foreach(_.send(ReviveOffers))
        },
        0,
        reviveIntervalMs,
        TimeUnit.MILLISECONDS
      )
    }

    override def receive: PartialFunction[Any, Unit] = {
      case StatusUpdate(taskId, state, data) =>
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {

        }
      case ReviveOffers =>
        makeOffers()
      case KillTask(taskId, interruptThread) =>
        //
      //case KillExecutorOnHost(host) =>
        //
      //case RemoveExecutor()
        //
      case LaunchedExecutor(executorId) =>
        makeOffers(executorId)
      case e =>
        logger.error(s"Received unexpected message. ${e}")
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterExecutor(executorId, hostname, cores) =>
        if (executorDataMap.contains(executorId)) {

        } else {

        }

      case StopExecutor =>
        //
      case IsExecutorAlive(executorId) =>
        context.reply(isExecutorActive(executorId))

      case e => 
        logger.error(s"Received unexpected ask ${e}")
    }

    private def makeOffers(): Unit = {

    }

    private def makeOffers(executorId: String): Unit = {

    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {

    }

    private def launchTasks(tasks: Seq[Seq[TaskDescription]]): Unit = {
      for (task <- tasks.flatten) {

      }
    }

    private def removeExecutor(executorId: String): Unit = {

    }
  } // end class DriverEndpoint

  val driverEndpoint = rpcEnv.setupEndpoint(
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME, 
    createDriverEndpoint()
  )

  protected def createDriverEndpoint(): DriverEndpoint = new DriverEndpoint()

  override def start(): Unit = {

  }

  override def stop(): Unit = {

  }

  override def reviveOffers(): Unit = Utils.tryLogNonFatalError {
    driverEndpoint.end(ReviveOffers)
  }

  override def killTask(
    taskId: Long,
    executorId: String,
    interruptThread: Boolean,
    reason: String
  ): Unit = {
    driverEndpoint.send(KilTask(taskId, executorId, interruptThread))
  }

  override def defaultParallelism(): Int = 2
}

object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}