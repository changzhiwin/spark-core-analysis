package xyz.sourcecodestudy.spark.executor

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal
import scala.util.{Success, Failure}

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.{SparkEnv, SparkConf, TaskState, MapOutputTrackerExecutor}
import xyz.sourcecodestudy.spark.rpc.{RpcEnv, RpcAddress, RpcEndpointRef, IsolatedRpcEndpoint, RpcCallContext}
import xyz.sourcecodestudy.spark.TaskState._
import xyz.sourcecodestudy.spark.util.{ThreadUtils, SerializableBuffer}
import xyz.sourcecodestudy.spark.scheduler.cluster.CoarseGrainedClusterMessage._

class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    hostname: String,
    cores: Int,
    env: SparkEnv) extends IsolatedRpcEndpoint with ExecutorBackend with Logging {

  val stopping = new AtomicBoolean(false)

  var executor: Option[Executor] = None

  var driver: Option[RpcEndpointRef] = None

  override def onStart(): Unit = {
    // get driverEndpontRef
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      driver = Some(ref)
      ref.ask[Boolean]( RegisterExecutor(executorId, self, hostname, cores) )
    }(ThreadUtils.sameThread).onComplete {
      case Success(_) =>
        self.send(RegisteredExecutor)
      case Failure(e) =>
        exitExecutor(1, s"Cannot register with deriver: ${driverUrl}", e)
    }(ThreadUtils.sameThread)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor =>
      logger.info("Successfully registered with driver")
      try {
        executor = Some(new Executor(executorId, false))
        driver.get.send(LaunchedExecutor(executorId))
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, s"Unable to create executor due to ${e.getMessage()}", e)
      }

    case LaunchTask(taskId, data) =>
      assert(executor != None, "Received LaunchTask but no executor")
      // 需要取data.value，因为被包裹了一层
      executor.get.launchTask(this, taskId, data.value)

    case KillTask(taskId, _, interruptThread) =>
      executor.get.killTask(taskId, interruptThread)

    case StopExecutor =>
      stopping.set(true)
      logger.info("Driver commanded a shutdown")
      self.send(ShutDown)

    case ShutDown =>
      stopping.set(true)
      new Thread("CoarseGrainedExecutorBackend-stop-executor") {
        override def run(): Unit = {
          executor match {
            case Some(exe) => exe.stop()
            case None      => System.exit(1)
          }
        }
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case e => logger.warn(s"Unexpected message to receiveAndReply, ${e}")
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (stopping.get()) {
      logger.info(s"Driver from ${remoteAddress} disconnected during shutdown")
    } else if (driver.exists(_.address.get == remoteAddress)) {
      exitExecutor(1, s"Driver ${remoteAddress} disassociated! Shutting down.", null)
    } else {
      logger.warn(s"An unknow (${remoteAddress}) driver address")
    }
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = {
    val msg = StatusUpdate(executorId, taskId, state, SerializableBuffer(data))

    if (TaskState.isFinished(state)) {
      // Can do somethig
    }

    driver match {
      case Some(ref) => ref.send(msg)
      case None      => logger.warn(s"")
    }
  }

  protected def exitExecutor(code: Int, reason: String, throwable: Throwable = null): Unit = {
    if (stopping.compareAndSet(false, true)) {
      val message = s"Executor self-exiting due to ${reason}"
      Option(throwable) match {
        case Some(_) => logger.error(message, throwable)
        case None => {
          logger.warn(s"Code(${code}), ${message}")
        }
      }

      self.send(ShutDown)
    } else {
      logger.info("Skip exiting executor, already asked to exit")
    }
  }

  def fetchMapOutStatuses(shuffleId: Int, reduceId: Int): Array[String] = {
    val msg = RquestMapOut(shuffleId, reduceId)

    driver match {
      case Some(ref) => {
        logger.info(s"Ask driver about mapout, ${msg}")
        val response = ref.askSync[ResponseMapOut](msg)
        logger.info(s"Ask come back, ${response}")
        response.statuses.toArray
      }
      case None      => {
        throw new IllegalStateException("Executor need request mapOut, but driver is None.")
      }
    }    
  }
}

object CoarseGrainedExecutorBackend {
  
  case object RegisteredExecutor

  case class Arguments(
      driverUrl: String,
      executorId: String,
      hostname: String,
      port: Int,
      cores: Int)

  def main(args: Array[String]): Unit = {

    val arguments = parseArguments(args)

    val driverConf = new SparkConf()
    driverConf.set("port", arguments.port.toString)

    val env = SparkEnv.create(driverConf, false, false)
    SparkEnv.set(env)

    val backend = new CoarseGrainedExecutorBackend(
        env.rpcEnv, 
        arguments.driverUrl, 
        arguments.executorId,
        arguments.hostname,
        arguments.cores, 
        env)

    env.mapOutputTracker.asInstanceOf[MapOutputTrackerExecutor].setBackend(backend)
    env.rpcEnv.setupEndpoint("Executor", backend)

    env.rpcEnv.awaitTermination()
  }

  private def parseArguments(args: Array[String]): Arguments = {
    var driverUrl: String = "spark://CoarseGrainedScheduler@127.0.0.1:9990"  // driver nettyRpcEnv use
    var executorId: String = "1"
    var hostname: String = "spark://127.0.0.1:9995"  // executor nettyRpcEnv use
    var port: Int = 9995
    var cores: Int = 2

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--port") :: value :: tail =>
          port = value.toInt
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          System.exit(-1)
      }
    }

    Arguments(driverUrl, executorId, hostname, port, cores)
  }
}