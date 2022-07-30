package xyz.sourcecodestudy.spark.scheduler

// Represents free resources available on an executor.
case class WorkerOffer(executorId: String, host: String, cores: Int)