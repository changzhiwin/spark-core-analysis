package xyz.sourcecodestudy.spark.scheduler

//import scala.reflect.ClassTag

import java.io.{ByteArrayOutputStream}
import java.nio.ByteBuffer

import xyz.sourcecodestudy.spark.TaskContext
import xyz.sourcecodestudy.spark.serializer.SerializerInstance
abstract class Task[T](val stageId: Int, val partitionId: Int) extends Serializable {

  @transient private   var taskThread: Thread = _
  @transient protected var context: TaskContext = _

  final def run(attemptId: Long): T = {
    context = new TaskContext(stageId, partitionId, attemptId)
    taskThread = Thread.currentThread()

    if (_killed) kill(interruptThread = false)
    
    runTask(context)
  }

  def runTask(context: TaskContext): T

  def preferredLocations: Seq[TaskLocation] = Nil

  @transient private var _killed: Boolean = false
  def killed: Boolean = _killed

  def kill(interruptThread: Boolean): Unit = {
    _killed = true
    if (context != null) {
      context.interrupted = true
    }
    if (interruptThread && taskThread != null) {
      taskThread.interrupt()
    }
  }
}

object Task {

  def serializeWithDependencies(
      task: Task[_],
      serializer: SerializerInstance): ByteBuffer = {
    //val dataOut = new DataOutputStream(out)

    //val out = new ByteArrayOutputStream(4096)
    //val taskBytes = serializer.serialize(task).array()
    //out.write(taskBytes)
    //ByteBuffer.wrap(out.toByteArray)
    serializer.serialize(task)
  }

  def deserializeWithDependencies(serializedTask: ByteBuffer): (ByteBuffer) = {
    
    //val in = new ByteBufferInputStream(serializedTask)
    
    // only have task info, right now
    (serializedTask)
  }

}