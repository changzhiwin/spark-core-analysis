package xyz.sourcecodestudy.spark.scheduler

import org.apache.logging.log4j.scala.Logging

import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}

import xyz.sourcecodestudy.spark.{SparkContext, SparkEnv}
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
  val resultStageToJob = new HashMap[Stage, ActiveJob]
  val jobIdToActiveJob = new HashMap[Int, ActiveJob]
  val activeJobs = new HashSet[ActiveJob]

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      parititions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit): Unit ={
    submitJob(rdd, func, parititions, allowLocal, resultHandler)
  }

  def submitJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit): Unit =
  {
    assert(partitions.size > 0)

    val maxPartitions = rdd.partitions.length

    val jobId = nextJobId.getAndIncrement()
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]

    // JobSubmitted, need to create new stage
    // val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)

    val finalStage = newStage(rdd, partitions.size, None, jobId)

    val job = new ActiveJob(jobId, finalStage, func2, partitions)

    if (allowLocal && finalStage.parents.size == 0 && partitions.length == 1) {
      // run locally
    } else {
      jobIdToActiveJob(jobId) = job
      activeJobs += job
      resultStageToJob(finalStage) = job
      submitStage(finalStage)
    }

    submitWaitingStages()
  }
  // Job Process end

  val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  val stageIdToJobIds = new HashMap[Int, HashSet[Int]]

  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    val allStage = stage :: getParentStages(stage.rdd, jobId)
    // TODO parent's parent
    allStage.foreach(s => {
      stageIdToJobIds.getOrElseUpdate(s.id, new HashSet[Int]) += jobId
      jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]) += s.id
    })
  }

  // Stage Process start
  val waitingStages = new HashSet[Stage]
  val runningStages = new HashSet[Stage]
  val failedStages = new HashSet[Stage]
  val stageIdToStage = new HashMap[Int, Stage]
  val shuffleToMapStage = new HashMap[Int, Stage]

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
    val visited = HashSet[Int]()
    val parents = HashSet[Stage]()

    def visit(r: RDD[_]): Unit = {
      if (!visited(r.id)) {
        visited += r.id
        for(d <- r.dependencies) {
          // Notice: 这是一个边界，与上游依赖切断了！！
          case _: ShuffleDependency[_, _] => parents += getShuffleMapStage(d, jobId)
          case _                          => visit(d.rdd)
        }
      }
    }
    visit(rdd)
    parents.toList
  }

  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val visited = HashSet[Int]()
    val missing = HashSet[Stage]()

    def visit(r: RDD[_]): Unit = {
      if (!visited[r.id]) {
        visited += r.id
        for(d <- r.dependencies) {
          case _: ShuffleDependency[_, _] => {
            val mapStage = getShuffleMapStage(d, stage.jobId)
            // 是否计算过，并且结果还保存着，条件（不是shuffle，或者完成了所有的partition计算）
            if (!mapStage.isAvailable) {
              missing += mapStage
            }
          }
          case _                          => visit(d.rdd)
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
    if (stageIdToJobIds.contains(stage.id)) {
      //jobId 是从小到大自增的，这里排序难道是考虑依赖？
      val jobsThatUseStage: Array[Int] = stageIdToJobIds(stage.id).toArray.sorted
      jobsThatUseStage.find(jobIdToActiveJob.contains)
    } else {
      None
    }
  }

  private def resubmitFailedStages(): Unit = {
    if (failedStages.size > 0) {
      logger.info("Resubmit failed stages")
      val failedStagesCopy = failedStages.toArray
      failedStages.clear
      failedStagesCopy.sortBy(_.jobId).foreach(stage => submitStage(stage))
    }
    submitWaitingStages()
  }

  private def submitWaitingStages(): Unit = {
    logger.info("Checking for newly runnable parent stages")
    val waitingStagesCopy = waitingStages.toArray
    waitingStages.clear
    waitingStagesCopy.sortBy(_.jobId).foreach(stage => submitStage(stage))
  }

  // First check missing parents
  def submitStage(stage: Stage): Unit = {
    activeJobForStage(stage) match {
      case Some(jobId) => {
        logger.info(s"submitStage(${stage})")
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
  val pendingTasks = new HashMap[Stage, HashSet[Task[_]]]

  def submitMissingTasks(stage: Stage, jobId: Int): Unit = {
    val myPending = pendingTasks.getOrUpdate(stage, new HashSet[Task[_]])
    myPending.clear()

    var tasks = ArrayBuffer[Task[_]]()
    if (stage.isShuffleMap) {
      //TODO
    } else {
      val job = resultStageToJob(stage)
      for (id <- 0 until job.numTotalJobs) {
        val partition = job.partitions(id)
        tasks += new ResultTask(stage.id, stage.rdd, job.func, partition, id)
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

    //CompletionEvent(task, reason, result, accumUpdates, taskInfo)
  }
  // Task Process end
}

object DAGScheduler {

  val RESUBMIT_TIMEOUT = 200.milliseconds

  val POLL_TIMEOUT = 10L

  // Warns the user if a stage contains a task with size greater than this value (in KB)
  val TASK_SIZE_TO_WARN = 100
}