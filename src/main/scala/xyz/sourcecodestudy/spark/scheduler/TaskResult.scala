package xyz.sourcecodestudy.spark.scheduler

import java.nio.ByteBuffer

import xyz.sourcecodestudy.spark.SparkEnv

sealed trait TaskResult[T]

// blockId mock, TODO
case class IndirectTaskResult[T](blockId: Long) extends TaskResult[T] with Serializable

// accumUpdates mock, TODO
class DirectTaskResult[T](val valueBytes: ByteBuffer, val accumUpdates: Map[Long, Any] = null) extends TaskResult[T] with Serializable {
  def this() = this(null.asInstanceOf[ByteBuffer], null)

  def value(): T = {
    val resultSer = SparkEnv.get.serializer.newInstance()
    resultSer.deserialize(valueBytes)
  }
}