package xyz.sourcecodestudy.spark.scheduler

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.apache.logging.log4j.scala.Logging

class TaskSetManager(
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet,
    maxTaskFailures: Int) extends Logging {

  // Vals
  val tasks = taskSet.tasks
  val numTasks = tasks.length
  val copiesRunning = new Array[Int](numTasks)
  val successful = new Array[Boolean](numTasks)
  val numFailures = new Array[Int](numTasks)
  val stageId = taskSet.stageId
  val name = s"TaskSet_${stageId}"

  // Record running
  val runningTaskSet = new HashSet[Long]

  def addRunningTask(taskId: Long): Unit = {
    runningTaskSet.add(taskId)
  }
  def removeRunningTask(taskId: Long): Unit = {
    runningTaskSet.remove(taskId)
  }

  override def runningTasks = runningTaskSet.size
  // End Record running

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
    //var indexOffset = allPendingTasks.size
    val idx = allPendingTasks.lastIndexWhere(taskIdx => copiesRunning(taskIdx) == 0 && !successful(taskIdx))
    idx match {
      case -1  => None
      case _   => Some(idx)
    }
  }

  // Respond to an offer of a single executor from the scheduler by finding a task
  def resourceOffer(execId: String, host: String): Option[TaskDescription] = {
    isZombie match {
      case false => {
        findTask(execId, host) match {
          case Some(index) => {
            val task = tasks(index)
            val taskId = sched.newTaskId()

            copiesRunning(index) += 1

            val serializedTask = Task.serializeWithDependencies(task)
            addRunningTask(taskId)

            val taskName = s"task ${taskSet.id}:${index}"
            Some(new TaskDescription(taskId, execId, taskName, index, serializedTask))
          }
          case _           => None
        }
      }
      case true  => None
    }
  }

}