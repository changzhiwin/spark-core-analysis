package xyz.sourcecodestudy.spark.scheduler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.{TaskEndReason, Success, TaskKilled, ExceptionFailure}
import xyz.sourcecodestudy.spark.TaskState

class TaskSetManager(
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet,
    maxTaskFailures: Int) extends Logging {

  // Vals
  val tasks = taskSet.tasks
  val numTasks = tasks.length
  //val copiesRunning = new Array[Int](numTasks)
  val successful = new Array[Boolean](numTasks)
  val numFailures = new Array[Int](numTasks)
  val taskInfos = new HashMap[Long, TaskInfo]
  val stageId = taskSet.stageId
  val name = s"TaskSet_${stageId}"

  // Record running task
  val runningTaskSet = new HashSet[Long]

  def addRunningTask(taskId: Long): Unit = {
    runningTaskSet.add(taskId)
  }
  def removeRunningTask(taskId: Long): Unit = {
    runningTaskSet.remove(taskId)
  }

  override def runningTasks = runningTaskSet.size
  // End Record running task

  // Record pending
  val allPendingTasks = new ArrayBuffer[Int]

  private def addPendingTask(index: Int, readding: Boolean = false): Unit = {
    readding match {
      case false => allPendingTasks += index
      case true  => 
    }
  }
  // End Record pending

  // In reverse order so that tasks with low indices get launched first.
  (0 until numTasks).reverse.foreach(i => addPendingTask(i))

  // Vars
  var tasksSucessful = 0
  var isZombie = false

  /**
    * 实现单个TaskSet里面的调度逻辑，这里只使用了最简单的实现，无特殊逻辑。
    * 发版版本实现考虑了最适合Task运行的环境，例如host，exector偏好
    * @param execId 保留，未使用
    * @param host 保留，未使用
    * @return Option[taskId]
    */
  private def findTask(execId: String, host: String): Option[Int] = {
    val idx = allPendingTasks.lastIndexWhere(taskIdx => !successful(taskIdx))
    idx match {
      case -1  => None
      case _   => {
        // 从队列取出
        allPendingTasks.remove(idx)
        Some(idx)
      }
    }
  }

  // Respond to an offer of a single executor from the scheduler by finding a task
  def resourceOffer(execId: String, host: String): Option[TaskDescription] = {
    isZombie match {
      case false => {
        findTask(execId, host) match {
          case Some(index) => {
            val task = tasks(index)

            // index在这个TaskSet里面是一一对应的
            val taskId = sched.newTaskId()
            val info = new TaskInfo(taskId, index, execId, host)
            taskInfos(taskId) = info

            val serializedTask = Task.serializeWithDependencies(task)
            addRunningTask(taskId)

            val taskName = s"task ${taskSet.id}:${taskId}:${index}"
            sched.dagScheduler.taskStarted(task, info)

            Some(new TaskDescription(taskId, execId, taskName, index, serializedTask))
          }
          case _           => None
        }
      }
      case true  => None
    }
  }

  // 需要考虑线程安全吧？？？ TODO
  def handleSuccessfulTask(taskId: Long, result: DirectTaskResult[_]): Unit = {

    val info = taskInfos(taskId)
    val index = info.index
    val host = info.host
    removeRunningTask(taskId)

    sched.dagScheduler.taskEnded(tasks(index), Success, result.value, null, info)

    successful(index) match {
      case false => {
        tasksSucessful += 1
        successful(index) = true
        isZombie = (tasksSucessful == numTasks)
        logger.info(s"Finished task(${taskId}), in taskSet(${name})")
      }
      case true  => 
        logger.info(s"Ignoring task-fininshed event for taskId ${taskId}, index ${index}, already completed successfully")
    }

    maybeFinishTaskSet()
  }

  def handleFailedTask(taskId: Long, state: TaskState, reason: TaskEndReason): Unit = {
    
    val info = taskInfos(taskId)
    val index = info.index
    val host = info.host

    removeRunningTask(taskId)   
    sched.dagScheduler.taskEnded(tasks(index), reason, null, null, info)
    logger.warn(s"Failed task(${taskId}), in taskSet(${name}) for reason ${reason}")

    // 重试逻辑，maxTaskFailures > numFailures(index)
    if (!isZombie && state != TaskState.KILLED) {
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {
        val message = s"Task ${taskId}:${index} failed ${maxTaskFailures} times; aborting job"
        logger.error(message)
        abort(message)
      } else {
        // 重试
        logger.warn(s"Task ${taskId}:${index} failed ${numFailures(index)} times;; retry")
        addPendingTask(index)
      }
    }

    maybeFinishTaskSet()
  }

  def abort(message: String): Unit = {
    sched.dagScheduler.taskSetFailed(taskSet, message)
    isZombie = true
    maybeFinishTaskSet()
  }

  private def maybeFinishTaskSet(): Unit = {
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
    }
  }
}