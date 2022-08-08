package xyz.sourcecodestudy.spark.serializer

import java.nio.ByteBuffer
import java.io.{InputStream, OutputStream}

import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.SparkEnv

trait Serializer {
  def newInstance(): SerializerInstance
}

object Serializer {
  def getSerializer(serializer: Serializer): Serializer = {
    if (serializer == null) SparkEnv.get.serializer else serializer
  }
}

/**
  * T: ClassTag is context bound
  * `def serialize[T: ClassTag]` is same as `def serialize[T](implicit c: ClassTag[T])`
  * The only allowed types for T are those for which a given ClassTag[T] exists in scope
  */
trait SerializerInstance {
  
  def serialize[T: ClassTag](t: T): ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream

  def deserializeStream(s: InputStream): DeserializationStream

}

trait SerializationStream {

  def writeObject[T: ClassTag](t: T): SerializationStream

  def flush(): Unit

  def close(): Unit

}

trait DeserializationStream {

  def readObject[T: ClassTag](): T

  def close(): Unit

}