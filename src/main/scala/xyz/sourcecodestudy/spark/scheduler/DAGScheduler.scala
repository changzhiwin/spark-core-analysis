package xyz.sourcecodestudy.spark.scheduler

import org.apache.logging.log4j.scala.Logging

import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}

import xyz.sourcecodestudy.spark.{ShuffleDependency, NarrowDependency}
import xyz.sourcecodestudy.spark.{SparkContext, SparkEnv, TaskEndReason, Success, TaskContext}
import xyz.sourcecodestudy.spark.rdd.RDD

class DAGScheduler(
    val sc: SparkContext,
    val taskScheduler: TaskScheduler,
    env: SparkEnv) extends Logging {

  def this(sc: SparkContext) = this(sc, sc.taskScheduler, sc.env)

  private val nextJobId = new AtomicInteger(0)
  def numTotalJobs: Int = nextJobId.get()
  private val nextStageId = new AtomicInteger(0)

  // Job Process start 
  val resultStageToJob = new HashMap[Stage, ActiveJob]  // 全局变量
  val jobIdToActiveJob = new HashMap[Int, ActiveJob]    // 全局变量
  val activeJobs = new HashSet[ActiveJob]               // 全局变量

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      parititions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit): Unit ={
    val waiter = submitJob(rdd, func, parititions, allowLocal, resultHandler)
    // 线程等待，处理main线程提前执行的问题
    waiter.awaitResult() match {
      case 0      => 
      case r: Int => logger.error("Failed run, awaitResult = -1")
    }
  }

  def submitJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      // resultHandler 没有被调用，导致collect失败，TODO
      resultHandler: (Int, U) => Unit): JobWaiter[U] =
  {
    assert(partitions.size > 0)

    // val maxPartitions = rdd.partitions.length

    val jobId = nextJobId.getAndIncrement()
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]

    // JobSubmitted, need to create new stage
    val finalStage = newStage(rdd, partitions.size, None, jobId)

    // resultHandler放到job上，当resultStage完成后，调用resultHandler
    val waiter = new JobWaiter[U](jobId, partitions.size, resultHandler)
    val job = new ActiveJob(jobId, finalStage, func2, partitions.toArray, waiter)

    if (allowLocal && finalStage.parents.size == 0 && partitions.length == 1) {
      // run locally
    } else {
      jobIdToActiveJob(jobId) = job
      activeJobs += job
      resultStageToJob(finalStage) = job
      submitStage(finalStage)
    }

    submitWaitingStages()
    waiter
  }
  // Job Process end

  val jobIdToStageIds = new HashMap[Int, HashSet[Int]]  // 全局变量
  val stageIdToJobIds = new HashMap[Int, HashSet[Int]]  // 全局变量

  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    val allStage = stage :: getParentStages(stage.rdd, jobId)
    // TODO parent's parent
    allStage.foreach(s => {
      stageIdToJobIds.getOrElseUpdate(s.id, new HashSet[Int]) += jobId
      jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]) += s.id
    })
  }

  // 整个状态管理逻辑，目前的理解还是有些困惑的。
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob, resultStage: Option[Stage]): Unit = {
    /**
     * 找job对应记录过stage集合registeredStages
     * 然后在stage和job的映射里面，使用registeredStages过滤，感觉像double check
     */
    jobIdToStageIds.get(job.jobId) match {
      case None                         => logger.error(s"No stages registered for job ${job.jobId}")
      case Some(emSet) if emSet.isEmpty => logger.error(s"Empty stages registered for job ${job.jobId}")
      case Some(registeredStages) =>
        stageIdToJobIds.filter(sId => registeredStages.contains(sId._1)).foreach {
          case (stageId, jobSet) =>
            if (!jobSet.contains(job.jobId)) {
              logger.error(s"Job ${job.jobId} not registered for stage ${stageId} even though that stage was registered for the job")
            } else {
              // 该job完成了
              jobSet -= job.jobId
              if (jobSet.isEmpty) {
                for (stage <- stageIdToStage.get(stageId)) {
                  if (runningStages.contains(stage)) {
                    logger.info(s"Removing running stage ${stageId}")
                    runningStages -= stage
                  }

                  // TODO shuffleToMapStage

                  if (pendingTasks.contains(stage) && !pendingTasks(stage).isEmpty) {
                    logger.debug(s"Removing pending status for stage ${stageId}")
                  }
                  pendingTasks -= stage

                  if (waitingStages.contains(stage)) {
                    logger.debug(s"Removing stage ${stageId} from waiting set.")
                    waitingStages -= stage
                  }

                  if (failedStages.contains(stage)) {
                    logger.debug(s"Removing stage ${stageId} from failed set.")
                    failedStages -= stage
                  }
                } // End for

                stageIdToStage -= stageId
                stageIdToJobIds -= stageId
                logger.debug(s"After removal of stage ${stageId}, remaining stages = ${stageIdToStage.size}") 
              }
            }
        }
    }

    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job

    if (resultStage.isEmpty) {
      // 通过stage查找ActiveJob，理论上应该只有一个
      val resultStagesForJob = resultStageToJob.keySet.filter(stage => resultStageToJob(stage).jobId == job.jobId)
      if (resultStagesForJob.size != 1) {
        logger.warn(s"${resultStagesForJob.size} result stages for job ${job.jobId} (expect exactly 1)")
      }
      resultStageToJob --= resultStagesForJob
    } else {
      resultStageToJob -= resultStage.get
    }
  }

  // Stage Process start
  val waitingStages = new HashSet[Stage]           // 全局变量
  val runningStages = new HashSet[Stage]           // 全局变量
  val failedStages = new HashSet[Stage]            // 全局变量
  val stageIdToStage = new HashMap[Int, Stage]     // 全局变量
  val shuffleToMapStage = new HashMap[Int, Stage]  // 全局变量

  // stage 是否依赖 target
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {

    val visitedRdds = new HashSet[RDD[_]]
    //val visitedStages = new HashSet[Stage]
    def visit(rdd: RDD[_]): Unit = {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _] =>
              val mapStage = getShuffleMapStage(shufDep, stage.jobId)
              if (!mapStage.isAvailable) {
                //visitedStages += mapStage
                visit(mapStage.rdd)
              }
            case narrowDep: NarrowDependency[_]   =>
              visit(narrowDep.rdd)
            case _                                =>
          }
        }
      }
    }

    (stage == target) match {
      case false =>
        visit(stage.rdd)
        visitedRdds.contains(target.rdd)
      case true  => true
    }
  }

  private def getShuffleMapStage(shuffleDep: ShuffleDependency[_, _], jobId: Int): Stage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None        =>
        val stage = newOrUsedStage(
          shuffleDep.rdd,
          shuffleDep.rdd.partitions.size,
          shuffleDep,
          jobId
        )
        shuffleToMapStage(shuffleDep.shuffleId) = stage
        stage
    }
  }

  private def getParentStages(rdd: RDD[_], jobId: Int): List[Stage] = {
    val visited = new HashSet[Int]
    val parents = new HashSet[Stage]

    def visit(r: RDD[_]): Unit = {
      if (!visited(r.id)) {
        visited += r.id
        for(d <- r.dependencies) {
          d match {
            // Notice: 这是一个边界，与上游依赖切断了！！
            case shufDep: ShuffleDependency[_, _] => parents += getShuffleMapStage(shufDep, jobId)
            case _                          => visit(d.rdd)
          }
        }
      }
    }
    visit(rdd)
    parents.toList
  }

  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val visited = new HashSet[Int]
    val missing = new HashSet[Stage]

    def visit(r: RDD[_]): Unit = {
      if (!visited(r.id)) {
        visited += r.id
        for(d <- r.dependencies) {
          d match {
            case shufDep: ShuffleDependency[_, _] => {
              val mapStage = getShuffleMapStage(shufDep, stage.jobId)
              // 是否计算过，并且结果还保存着，条件（不是shuffle，或者完成了所有的partition计算）
              if (!mapStage.isAvailable) {
                missing += mapStage
              }
            }
            case _                                => visit(d.rdd)
          }
        }
      }
    }

    visit(stage.rdd)
    missing.toList
  }

  private def newStage(
    rdd: RDD[_],
    numTasks: Int,
    shuffleDep: Option[ShuffleDependency[_, _]],
    jobId: Int): Stage = {

    val id = nextStageId.getAndIncrement()
    val stage = new Stage(id, rdd, numTasks, shuffleDep, getParentStages(rdd, jobId), jobId)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  private def newOrUsedStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: ShuffleDependency[_, _],
      jobId: Int): Stage = {
    val stage = newStage(rdd, numTasks, Some(shuffleDep), jobId)
    //TODO: mapOutputTracker registe
    stage
  }

  // submitStage为什么先要调这一步，没看懂？？？返回的是jobId
  private def activeJobForStage(stage: Stage): Option[Int] = {
    // 找到这个stage关联的所有jobId
    if (stageIdToJobIds.contains(stage.id)) {
      // 把jobId从小到大排序
      val jobsThatUseStage: Array[Int] = stageIdToJobIds(stage.id).toArray.sorted
      // 找到第一个jobId处于jobIdToActiveJob中的job
      jobsThatUseStage.find(jobIdToActiveJob.contains)
    } else {
      None
    }
  }

  /*
  private def resubmitFailedStages(): Unit = {
    if (failedStages.size > 0) {
      logger.info("Resubmit failed stages")
      val failedStagesCopy = failedStages.toArray
      failedStages.clear
      failedStagesCopy.sortBy(_.jobId).foreach(stage => submitStage(stage))
    }
    submitWaitingStages()
  }
  */

  private def submitWaitingStages(): Unit = {
    logger.info("Checking for newly runnable parent stages")
    val waitingStagesCopy = waitingStages.toArray
    waitingStages.clear()
    waitingStagesCopy.sortBy(_.jobId).foreach(stage => submitStage(stage))
  }

  // First check missing parents
  def submitStage(stage: Stage): Unit = {
    activeJobForStage(stage) match {
      case Some(jobId) => {
        logger.info(s"submitStage(${stage}) in jobId ${jobId}")
        if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
          // 查找依赖的Stage，是否有未完成的
          val missing = getMissingParentStages(stage).sortBy(_.id)
          logger.info(s"missing: ${missing}")

          if (missing == Nil) {
            logger.info(s"Submitting ${stage} (${stage.rdd}), which has no missing parents")
            // Do real thing
            submitMissingTasks(stage, jobId)
            runningStages += stage
          } else {
            // 有依赖未完，先执行依赖。这是一个递归的过程
            missing.foreach(p => submitStage(p))
            waitingStages += stage
          }
        }
      }
      case None    => logger.warn(s"No active job for stage ${stage.id}")
    }
  }
  // Stage Process end

  // Task Process start
  val pendingTasks = new HashMap[Stage, HashSet[Task[_]]]  // 全局变量

  def submitMissingTasks(stage: Stage, jobId: Int): Unit = {
    val myPending = pendingTasks.getOrElseUpdate(stage, new HashSet[Task[_]])
    myPending.clear()  // 这个pending，每次都清空，因为这里的key是stage

    val tasks = ArrayBuffer[Task[_]]()
    if (stage.isShuffleMap) {
      //TODO
    } else {
      val job = resultStageToJob(stage)
      for (id <- 0 until job.numPartitions if !job.finished(id)) {
        val partition = job.partitions(id)
        tasks += new ResultTask(stage.id, stage.rdd, job.func, partition, id) // (stageId, rdd, func, partition, outputId)
      }
    }

    myPending ++= tasks
    taskScheduler.submitTasks(
      new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId)
    )
  }

  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Map[Long, Any],
      taskInfo: TaskInfo): Unit = {
    // 维护的清单：
    // 1,  pendingTasks: HashMap[Stage, HashSet[Task[_]]]   需要更新管理，checked
    // 2,  waitingStages: HashSet[Stage]                    维护方: submitWaitingStages
    // 3,  runningStages: HashSet[Stage]                    需要更新管理，checked
    // 4,  failedStages: HashSet[Stage]                     维护方: resubmitFailedStages
    // 5,  stageIdToStage: HashMap[Int, Stage]              维护方: newStage会建立id到对象的引用
    // 6,  shuffleToMapStage: HashMap[Int, Stage]           维护方: shuffleId和stage的映射，是stageIdToStage的子集
    // 7,  jobIdToStageIds: HashMap[Int, HashSet[Int]]      check, cleanupStateForJobAndIndependentStages
    // 8,  stageIdToJobIds: HashMap[Int, HashSet[Int]]      check, cleanupStateForJobAndIndependentStages

    // 9,  resultStageToJob: HashMap[Stage, ActiveJob]      维护方: submitJob里面建立了stage/jobId/job初始关系
    // 10, jobIdToActiveJob: HashMap[Int, ActiveJob]        需要更新管理，check
    // 11, activeJobs: HashSet[ActiveJob]                   需要更新管理，check
    
    // 问题：如何确定一个Stage完成了？ 如何确定一个Job完成了？
    // 回答：
    // 每一个ResultTask完，表示一个分区任务完成，看是否完成了所有分区的计算；
    // 并且resultStage是和AciveJob一一对应的，依赖的Stage都是ShuffleMapTask

    //CompletionEvent(task, reason, result, accumUpdates, taskInfo)
    val stageId = task.stageId
    val stage = stageIdToStage(stageId)

    reason match {
      case Success =>
        logger.info(s"Completed ${task}")
        pendingTasks(stage) -= task

        task match {
          case rt: ResultTask[_, _] =>
            resultStageToJob.get(stage) match {
              case Some(job) =>
                //判断这个分区是否完成，outputId
                if (!job.finished(rt.outputId)) {
                  job.finished(rt.outputId) = true
                  job.numFinished += 1

                  // 理论上完成的这个分区就是outputId
                  job.listener.taskSucceeded(rt.outputId, result)

                  // 如果这个job完成了，对全局状态进行维护
                  if (job.numFinished == job.numPartitions) {
                    runningStages -= stage
                    cleanupStateForJobAndIndependentStages(job, Some(stage))
                  }
                }
              case None      =>
                logger.info(s"Ignoring result from ${rt} because its job has finished")
            }
          //case smt: ShuffleMapTask  =>
          case _                     =>
        }
      case _       =>

    }
    submitWaitingStages()
  }

  def taskStarted(task: Task[_], taskInfo: TaskInfo): Unit = {
    submitWaitingStages()
  }

  def taskGettingResult(taskInfo: TaskInfo): Unit = {
    submitWaitingStages()
  }

  // 查找失败的Stage，以及依赖该Stage的Stage，逐一取消task的运行
  def taskSetFailed(taskSet: TaskSet, message: String): Unit = {

    val stageId = taskSet.stageId

    // 只处理注册过的
    if (stageIdToStage.contains(stageId)) {
      val failedStage = stageIdToStage(stageId)
      val dependentStages = resultStageToJob.keys.filter(x => stageDependsOn(x, failedStage)).toSeq

      for (resultStage <- dependentStages) {
        val job = resultStageToJob(resultStage)
        val stages = jobIdToStageIds(job.jobId)

        for (sId <- stages) {
          val stage = stageIdToStage(sId)
          if (runningStages.contains(stage)) {
            taskScheduler.cancelTasks(sId, true)
          }
        }
      }
    }
  }
  // Task Process end
}

object DAGScheduler {

  // val RESUBMIT_TIMEOUT = 200.milliseconds

  val POLL_TIMEOUT = 10L

  // Warns the user if a stage contains a task with size greater than this value (in KB)
  val TASK_SIZE_TO_WARN = 100
}