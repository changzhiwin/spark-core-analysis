package xyz.sourcecodestudy.spark

import scala.collection.mutable.ArrayBuffer

class TaskContext(
    val stageId: Int,
    val partitionId: Int,
    val attemptId: Long,
    val runningLocally: Boolean = false)
  extends Serializable {

  private val onCompleteCallbacks = new ArrayBuffer[() => Unit]

  var interrupted: Boolean = false  

  var completed: Boolean = false

  def addOnCompleteCallback(f: () => Unit): Unit = {
    onCompleteCallbacks += f
  }

  def executeOnCompleteCallbacks(): Unit = {
    completed = true
    onCompleteCallbacks.reverse.foreach { _() }
  }

}