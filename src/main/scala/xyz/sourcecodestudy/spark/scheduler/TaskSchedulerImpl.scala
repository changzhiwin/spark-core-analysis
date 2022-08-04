package xyz.sourcecodestudy.spark.scheduler

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import scala.util.Random
import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.{TaskEndReason}
import xyz.sourcecodestudy.spark.{SparkContext, SparkEnv, TaskState}
import xyz.sourcecodestudy.spark.TaskState.TaskState

class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging 
{
  def this(sc: SparkContext) = this(sc, 1)

  val conf = sc.conf

  val activeTaskSets = new HashMap[String, TaskSetManager]
  val taskIdToTaskSetId = new HashMap[Long, String]

  // 暂时只有一个Executor，先不实现
  //val taskIdToExecutorId = new HashMap[Long, String]
  //val activeExecutorIds = new HashSet[String]

  val nextTaskId = new AtomicLong(0)
  def newTaskId(): Long = nextTaskId.getAndIncrement()

  var dagScheduler: DAGScheduler = null
  override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = this.dagScheduler = dagScheduler

  var backend: SchedulerBackend = null
  def initialize(backend: SchedulerBackend): Unit = {
    this.backend = backend
  }

  override def start(): Unit = {
    backend.start()
  }

  /**
    * 触发顺序：SparkContext.runJob -> 
              DAGScheduler.(runJob -> submitJob -> submitStage -> submitMissingTasks) -> 
              TaskScheduler.submitTasks
    */
  override def submitTasks(taskSet: TaskSet): Unit = {

    val tasks = taskSet.tasks
    logger.info(s"Adding task set ${taskSet.id} with ${tasks.length} tasks")

    this.synchronized {
      val manager = new TaskSetManager(this, taskSet, maxTaskFailures = 0 )
      activeTaskSets(taskSet.id) = manager
      // 注意：先忽略复杂的调度实现，只维护一个激活状态的TaskSet集合
    }

    // 触发执行
    backend.reviveOffers()
  }
  
  // 对应上面activeTaskSets的新增
  def taskSetFinished(manager: TaskSetManager): Unit = {
    synchronized {
      activeTaskSets -= manager.taskSet.id
      logger.info(s"TaskSet ${manager.taskSet.id}, whose tasks have all completed")
    }
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logger.info(s"Cancelling stage ${stageId}")
    
    activeTaskSets.find(_._2.stageId == stageId).foreach {
      case (_, tsm) => {
        tsm.runningTaskSet.foreach { taskId => backend.killTask(taskId, interruptThread) }

        val message = s"Stage ${stageId} was cancelled"
        tsm.abort(message)
        logger.info(message)
      }
    }
  }

  override def stop(): Unit = {
    if (backend != null) backend.stop()

    Thread.sleep(1000L)
  }

  /**
    * 触发顺序：TaskScheduler.submitTasks -> Backend.reviveOffers -> TaskScheduler.resourceOffers
    * 在Backend真正需要运行线程时调用，告诉TaskScheduler现在可用的资源情况，申请运行一些任务
    */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {

    // 这里本意是会拿到一个Worker的列表，包含可用的CPU核
    val shuffledOffers = Random.shuffle(offers)
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))

    /**
      * 拿activeTaskSets: HashMap[String, TaskSetManager]实现简单调度
      * key = taskSet.id = stageId + "." + attempt
      * 我理解taskSet其实是和stage有对应关系的：同一个job里面是一一对应
      * 按照key排序，这样stage的FIFO
      */
    // 调度的taskSet
    activeTaskSets.keys.toSeq.sorted.foreach(taskSetId => {
      val taskSetMgr = activeTaskSets(taskSetId)
      
      // 传入可用资源列表，在这些节点上执行
      shuffledOffers.zipWithIndex.foreach { case (offer, idx) =>

        for (taskDesc <- taskSetMgr.resourceOffer(offer.executorId, offer.host)) {
          tasks(idx) += taskDesc

          val taskId = taskDesc.taskId
          taskIdToTaskSetId(taskId) = taskSetMgr.taskSet.id
        }
      }
    })

    tasks.map(t => t.toSeq)
  }

  /**
    * 需要仔细处理来自Executor执行的结果：成功、异常（逻辑异常、物理异常）
    * 因为这里也是回调DAGScheduler，来更新Job、Stage、Task执行的情况的
    * 
    * 注意：Executor会启多线程来执行Task，会被不同线程回调，所以这个方法务必需要保证线程安全
    */
  def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer): Unit = {
    //  维护的全局状态
    //  activeTaskSets: HashMap[String, TaskSetManager]
    //  taskIdToTaskSetId: HashMap[Long, String]

    synchronized {
      logger.info(s"statusUpdate call taskId ${taskId}, state = ${state}")
      try {
        taskIdToTaskSetId.get(taskId) match {
          case Some(taskSetId) =>
            // Kill/Exception/Success都属于完成的状态
            if (TaskState.isFinished(state)) {
              taskIdToTaskSetId.remove(taskId)               // Task成功，移除跟踪
            }
            activeTaskSets.get(taskSetId) match {
              case Some(taskSetMgr) =>
                state match {
                  // 无异常，运行完成
                  case TaskState.FINISHED =>
                    taskSetMgr.handleSuccessfulTask(taskId, new DirectTaskResult(serializedData, null))
                  case TaskState.RUNNING  =>
                    logger.info(s"Task(${taskId}) is running state")
                  case _                  =>
                    // 需要反序列化成TaskEndReason对象，当前支持了TaskKilled, ExceptionFailure
                    // TODO，反序列化有可能失败
                    val reason = SparkEnv.get.closureSerializer.newInstance()
                      .deserialize[TaskEndReason](serializedData, Thread.currentThread.getContextClassLoader)
                    taskSetMgr.handleFailedTask(taskId, state, reason)
                }
              case _                => 
                logger.warn(s"Don't know why: task(${taskId}) success, but not found activeTaskSet by Id(${taskSetId})")
            }

          case None            =>
            logger.warn(s"Don't know why: task(${taskId}) success, but not found taskIdToTaskSetId")
        }
      } catch {
        // t.toString, t.getStackTrace
        case e: Exception => logger.error(s"Exception in statusUpdate, ${e.toString}, ${e.getStackTrace}")
      }
    }
  }

  override def defaultParallelism() = backend.defaultParallelism()
}