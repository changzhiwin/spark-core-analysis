package xyz.sourcecodestudy.spark.scheduler

import org.apache.logging.log4j.scala.Logging

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

  private var dagScheduler: DAGScheduler = null
  override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = this.dagScheduler = dagScheduler

  private var backend: SchedulerBackend = null
  def initialze(backend: SchedulerBackend): Unit = {
    this.backend = backend
  }

  override def start(): Unit = {
    backend.start()
  }

  /**
    * 触发顺序：SparkContext.runJob-> DAGScheduler.(runJob -> submitJob -> submitStage -> submitMissingTasks) -> TaskScheduler.submitTasks
    */
  override def submitTasks(taskSet: TaskSet): Unit = {

    val tasks = taskSet.tasks
    logger.info(s"Adding task set ${taskSet.id} with ${tasks.length} tasks")

    this.synchronized {
      val manager = new TaskSetManager(this, taskSet, maxTaskFailures = 0 )
      activeTaskSets(taskSet.id) = manager
      // 注意：先忽略复杂的调度实现，只维护一个激活状态的TaskSet集合
    }

    // 触发执行，实际会调用下面的resourceOffers()
    backend.reviveOffers()
  }
  
  // 对应上面activeTaskSets的新增
  def taskSetFinished(manager: TaskSetManager): Unit = {
    synchronized {
      activeTaskSets -= manager.taskSet.id
      logger.info(s"TaskSet ${manager.taskSet.id}, whose tasks have all completed")
    }
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = {
    logger.info(s"Cancelling stage ${stageId}")
    // TODO
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
      * 按照key排序，这样stage的FIFO
      */
    
    tasks
  }

  /**
    * 需要仔细处理来自Executor执行的结果：成功、异常（逻辑异常、物理异常）
    * 因为这里也是回调DAGScheduler，来更新Job、Stage、Task执行的情况的
    * 
    * 注意：Executor会启多线程来执行Task，会被不同线程回调，所以这个方法务必需要保证线程安全
    */
  def statusUpdate(tId: Long, state: TaskState, serializedData: ByteBuffer): Unit = {

    var failedExector: Option[String] = None
    synchronized {
      try {
        taskIdToTaskSetId.get(tId) match {
          case Some(taskSetId) =>
            if (TaskState.isFinished(state)) {

            }

          case None            =>

        }
      } catche {
        case e: Exception => logger.error(s"Exception in statusUpdate, ${e}")
      }
    }

    if (failedExector.isDefined) {

    }
  }

  override def defaultParallelism() = backend.defaultParallelism()
}