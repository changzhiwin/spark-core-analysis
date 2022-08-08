package xyz.sourcecodestudy.spark

import java.io.FileInputStream
import org.apache.logging.log4j.scala.Logging
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.serializer.Serializer
import xyz.sourcecodestudy.spark.{TaskContext}
import xyz.sourcecodestudy.spark.util.Utils

class FileStoreShuffleFetcher extends ShuffleFetcher with Logging {

  override def fetch[T: ClassTag](shuffleId: Int, reduceId: Int, context: TaskContext, serializer: Serializer): Iterator[T] = {

    //serializer的使用
    //serializer.newInstance().deserializeStream(stream).asIterator

    //怎么拿到有多少个数据分片了？老版本里面是使用MapOutputTracker记录了有多少个分片，调用getServerStatuses得到分片集合

    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    val ser = serializer.newInstance()
    
    // 收集结果
    val result = ArrayBuffer[T]()

    //for ((address, index) <- statuses.zipWithIndex) {
    for (index <- 0 until statuses.size) {
      val file = Utils.getShuffledOutputFile(shuffleId, index, reduceId)

      val in = ser.deserializeStream(new FileInputStream(file))

      try {
        val size = in.readObject[Int]()
        logger.info(s"Shuffle Read: elem [${size}], file [${file}]")
        for (i <- 0 until size) {
          result += in.readObject[T]()
        }
      } finally {
        in.close()
      }
    }

    // 返回结果
    result.iterator
  }
}