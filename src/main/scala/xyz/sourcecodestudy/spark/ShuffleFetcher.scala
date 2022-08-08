package xyz.sourcecodestudy.spark

import scala.reflect.ClassTag
import xyz.sourcecodestudy.spark.serializer.Serializer

abstract class ShuffleFetcher {
  def fetch[T: ClassTag](
      shuffleId: Int,
      reduceId: Int,
      context: TaskContext,
      serializer: Serializer): Iterator[T]

  def stop(): Unit = {}
}