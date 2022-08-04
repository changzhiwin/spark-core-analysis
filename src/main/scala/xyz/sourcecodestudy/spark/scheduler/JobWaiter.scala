package xyz.sourcecodestudy.spark.scheduler

class JobWaiter[T](
    //dagScheduler: DAGScheduler,
    jobId: Int,
    totalTasks: Int,
    resultHandler: (Int, T) => Unit) extends JobListener{

  private var finishedTasks = 0

  private var jobResult = -1

  def jobFinished: Boolean = (finishedTasks == totalTasks)

  override def taskSucceeded(index: Int, result: Any): Unit = synchronized {
    val r = result.asInstanceOf[T]
    resultHandler(index, r)
    finishedTasks += 1

    if (jobFinished) {
      jobResult = 0
      this.notifyAll()
    } 
  }

  override def jobFailed(exception: Exception): Unit = synchronized {
    finishedTasks = totalTasks
    this.notifyAll()
  }

  def awaitResult(): Int = synchronized {
    while (!jobFinished) {
      this.wait()
    }
    return jobResult
  }
}