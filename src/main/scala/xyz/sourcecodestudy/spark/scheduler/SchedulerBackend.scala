package xyz.sourcecodestudy.spark.scheduler

trait SchedulerBackend {

  private val appId = s"spark-application-${System.currentTimeMillis}"

  def start(): Unit
  def stop(): Unit
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  def killTask(
      taskId: Long,
      executorId: String,
      interruptThread: Boolean,
      reason: String): Unit = throw new UnsupportedOperationException

  def isReady(): Boolean = true

  def applicationId(): String = appId
}