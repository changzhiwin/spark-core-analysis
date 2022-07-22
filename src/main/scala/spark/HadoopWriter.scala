package org.apache.hadoop.mapred

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text

import java.text.SimpleDateFormat
import java.text.NumberFormat
import java.io.IOException
import java.net.URI
import java.util.Date

import spark.Logging
import spark.SerializableWritable

/**
 * Saves an RDD using a Hadoop OutputFormat as specified by a JobConf. The JobConf should also 
 * contain an output key class, an output value class, a filename to write to, etc exactly like in 
 * a Hadoop job.
 */
class HadoopWriter(@transient jobConf: JobConf) extends Logging with HadoopMapRedUtil with Serializable {
  
  private val now = new Date()
  private val conf = new SerializableWritable(jobConf)
  
  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  @transient private var writer: RecordWriter[AnyRef,AnyRef] = null
  @transient private var format: OutputFormat[AnyRef,AnyRef] = null
  @transient private var committer: OutputCommitter = null
  @transient private var jobContext: JobContext = null
  @transient private var taskContext: TaskAttemptContext = null

  def preSetup() {
    setIDs(0, 0, 0)
    setConfParams()
    
    val jCtxt = getJobContext() 
    getOutputCommitter().setupJob(jCtxt) 		
  }


  def setup(jobid: Int, splitid: Int, attemptid: Int) {
    setIDs(jobid, splitid, attemptid)
    setConfParams() 
  }

  def open() {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)
    
    val outputName = "part-"  + numfmt.format(splitID)
    val path = FileOutputFormat.getOutputPath(conf.value)
    val fs: FileSystem = {
      if (path != null) {
        path.getFileSystem(conf.value)
      } else {
        FileSystem.get(conf.value)
      }
    }

    getOutputCommitter().setupTask(getTaskContext()) 
    writer = getOutputFormat().getRecordWriter(
        fs, conf.value, outputName, Reporter.NULL)
  }

  def write(key: AnyRef, value: AnyRef) {
    if (writer!=null) {
      //println (">>> Writing ("+key.toString+": " + key.getClass.toString + ", " + value.toString + ": " + value.getClass.toString + ")")
      writer.write(key, value)
    } else {
      throw new IOException("Writer is null, open() has not been called")
    }
  }

  def close() {
    writer.close(Reporter.NULL)
  }

  def commit() {
    val taCtxt = getTaskContext()
    val cmtr = getOutputCommitter() 
    if (cmtr.needsTaskCommit(taCtxt)) {
      try {
        cmtr.commitTask(taCtxt)
        logInfo (taID + ": Committed")
      } catch {
        case e: IOException => { 
          logError("Error committing the output of task: " + taID.value, e)
          cmtr.abortTask(taCtxt)
          throw e
        }
      }   
    } else {
      logWarning ("No need to commit output of task: " + taID.value)
    }
  }

  def cleanup() {
    getOutputCommitter().cleanupJob(getJobContext())
  }

  // ********* Private Functions *********

  private def getOutputFormat(): OutputFormat[AnyRef,AnyRef] = {
    if (format == null) {
      format = conf.value.getOutputFormat()
        .asInstanceOf[OutputFormat[AnyRef,AnyRef]]
    }
    return format 
  }

  private def getOutputCommitter(): OutputCommitter = {
    if (committer == null) {
      committer = conf.value.getOutputCommitter().asInstanceOf[OutputCommitter]
    }
    return committer
  }

  private def getJobContext(): JobContext = {
    if (jobContext == null) { 
      jobContext = newJobContext(conf.value, jID.value)
    }
    return jobContext
  }

  private def getTaskContext(): TaskAttemptContext = {
    if (taskContext == null) {
      taskContext =  newTaskAttemptContext(conf.value, taID.value)
    }
    return taskContext
  }

  private def setIDs(jobid: Int, splitid: Int, attemptid: Int) {
    jobID = jobid
    splitID = splitid
    attemptID = attemptid

    jID = new SerializableWritable[JobID](HadoopWriter.createJobID(now, jobid))
    taID = new SerializableWritable[TaskAttemptID](
        new TaskAttemptID(new TaskID(jID.value, true, splitID), attemptID))
  }

  private def setConfParams() {
    conf.value.set("mapred.job.id", jID.value.toString);
    conf.value.set("mapred.tip.id", taID.value.getTaskID.toString); 
    conf.value.set("mapred.task.id", taID.value.toString);
    conf.value.setBoolean("mapred.task.is.map", true);
    conf.value.setInt("mapred.task.partition", splitID);
  }
}

object HadoopWriter {
  def createJobID(time: Date, id: Int): JobID = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    return new JobID(jobtrackerID, id)
  }
  
  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    var outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (outputPath == null || fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath = outputPath.makeQualified(fs)
    return outputPath
  }
}
