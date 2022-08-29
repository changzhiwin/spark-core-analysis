package xyz.sourcecodestudy.spark.executor

import org.apache.logging.log4j.scala.Logging
import java.util.concurrent.ConcurrentHashMap
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

import xyz.sourcecodestudy.spark.{SparkEnv, SparkConf}
import xyz.sourcecodestudy.spark.rpc.{RpcEnv, RpcEndpointRef}
import xyz.sourcecodestudy.spark.TaskState.TaskState
import xyz.sourcecodestudy.spark.util.ThreadUtils
import scala.util.control.NonFatal.apply

class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    //executorId: String,
    //host: String,
    //port: Int,
    env: SparkEnv) extends IsolatedRpcEndpoint with ExecutorBackend with Logging {

  val stopping = new AtomicBoolean(false)

  var executor: Option[Executor] = None

  var driver: Option[RpcEndpointRef] = None

  override def onStart(): Unit = {
    // get driverEndpontRef
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).falatMap { ref =>
      driver = Some(ref)
      ref.ask[Boolean](
        //TODO
        RegisterExecutor(???)
      )
    }(ThreadUtils.sameThread).onComplete {
      case Success(_) =>
        self.send(RegisteredExecutor)
      case Failure(e) =>
        // exit TODO
    }(ThreadUtils.sameThread)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor =>
      logger.info("Successfully registered with driver")
      try {

      } catch {
        case NonFatal(e) =>
          //exit TODO
      }

    case LaunchTask(data) =>
      assert(executor != None, "Received LaunchTask but no executor")
      val taskDesc = TaskDescription.
      executor.launchTask(this,taskDesc.taskId, )

    case KillTask(taskId, _, interruptThread, reason) =>
      executor.killTask(taskId, interruptThread)

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

  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (stopping.get()) {
      logger.info(s"")
    } else if (driver.exits(_.address.get == remoteAddress)) {
      // exit
    } else {
      logger.warn(s"")
    }
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = {
    val msg = StatusUpdate()
    driver match {
      case Some(ref) => ref.send(msg)
      case None      => logger.warn(s"")
    }
  }
}

object CoarseGrainedExecutorBackend {
  
  case object RegisteredExecutor

  case class Arguments(
      driverUrl: String,
      //executorId: String,
      host: String,
      port: Int,
      cores: Int,
      appId: String,
      workerUrl: Option[String])

  def main(args: Array[String]): Unit = {

    val arguments = parseArguments(args)

    val driverConf = new SparkConf()
    driverConf.set("port", argarguments.port.toString)

    val env = SparkEnv.create(driverConf, false, false) //createExecutorEnv()

    val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, arguments.driverUrl, env)

    env.rpcEnv.setupEndpoint("Executor", backend)

    env.rpcEnv.awaitTermination()
  }

  private parseArguments(args: Array[String]): Arguments = {
    var driverUrl: String = null
    var host: String = null
    var port: Int = 0
    var appId: String = null
    var cores: Int = 0
    var workerUrl: Option[String] = None

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--host") :: value :: tail =>
          host = value
          argv = tail
        case ("--port") :: value :: tail =>
          port = value.toInt
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          workerUrl = Some(value)
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          System.exit(-1)
      }
    }

    Arguments(driverUrl, host, port, cores, appId, workerUrl)
  }
}