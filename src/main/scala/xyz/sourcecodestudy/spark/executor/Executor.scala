package xyz.sourcecodestudy.spark.executor

import org.apache.logging.log4j.scala.Logging
import java.util.concurrent.ConcurrentHashMap
import java.nio.ByteBuffer

import xyz.sourcecodestudy.spark.{TaskKilledException, TaskKilled, ExceptionFailure, TaskState}
import xyz.sourcecodestudy.spark.util.Utils
import xyz.sourcecodestudy.spark.schedulerscheduler.Task

class Executor(executorId: String, isLocal: Boolean = false) extends Logging {

  logger.info(s"Start a executor")

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val threadPool = Utils.newDaemonCachedThreadPool("Executor task lauch worker")

  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  def launchTask(backend: SchedulerBackend, taskId: Long, serializedTask: ByteBuffer): Unit = {
    val tr = new TaskRunner(backend, taskId, serializedTask)
    runningTasks.put(taskId, tr)
    threadPool.execute(tr)
  }

  def killTask(taskId: Long, interruptThread: Boolean): Unit = {
    val tr = Option[TaskRunner](runningTasks.get(taskId))
    tr match {
      case Some(_) => tr.kill(interruptThread)
      case None     =>
    }
  }

  // TaskRunner define
  class TaskRunner(backend: SchedulerBackend, taskId: Long, serializedTask: ByteBuffer) extends Runnable {

    private var killed = false
    private var task: Task[Any] = _
    
    def kill(interruptThread: Boolean): Unit = {
      killed = true
      if (task != null) task.kill(interruptThread)
    }
    override def run(): Unit = {
      logger.info(s"Running task ID ${taskId}")
      backend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)

      val ser = SparkEnv.get.closureSerializer.newInstance()
      val resultSer = SparkEnv.get.serializer.newInstance()

      try {
        task = ser.deserialize[Task[Any]](serializedTask, Thread.currentThread.getContextClassLoader)

        if (killed) throw new TaskKilledException

        // 返回值，序列化后传参
        val value = task.run(taskId.toInt)

        if (task.killed) throw new TaskKilledException

        val valueBytes = resultSer.serialize(value)

        backend.statusUpdate(taskId, TaskState.FINISHED, valueBytes)

        logger.info(s"Finished task ID ${taskId}")

      } catch {
        /*
        case ffe: FetchFailedException => {
          logger.info(s"Executor Fetch failed task ${taskId}")
          val reason = ffe.toTaskEndReason
          backend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))
        }
        */
        case _: TaskKilledException | _: InterruptedException if task.killed => {
          logger.info(s"Executor killed task ${taskId}")
          backend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))
        }
        case t: Throwable => {
          logger.error(s"Exeception in task ID ${taskId}")
          val reason = ExceptionFailure(t.getClass.getName, t.toString, t.getStackTrace)
          backend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))
        }
      } finally {
        runningTasks.remove(taskId)
      }
    }
  }
}