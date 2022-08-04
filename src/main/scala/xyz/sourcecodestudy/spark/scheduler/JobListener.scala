package xyz.sourcecodestudy.spark.scheduler

trait JobListener {
  def taskSucceeded(index: Int, result: Any): Unit

  def jobFailed(exception: Exception): Unit
}