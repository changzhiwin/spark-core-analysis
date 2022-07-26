package org.apache.hadoop.mapred

trait HadoopMapRedUtil {
  def newJobContext(conf: JobConf, jobId: JobID): JobContext = new JobContext(conf, jobId)

  def newTaskAttemptContext(conf: JobConf, attemptId: TaskAttemptID): TaskAttemptContext = new TaskAttemptContext(conf, attemptId)
}
