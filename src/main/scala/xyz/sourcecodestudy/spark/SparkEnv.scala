package xyz.sourcecodestudy.spark

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.rpc.{RpcEnv}
import xyz.sourcecodestudy.spark.serializer.Serializer
import xyz.sourcecodestudy.spark.util.Utils

class SparkEnv(
    val rpcEnv: RpcEnv,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val shuffleFetcher: ShuffleFetcher,
    val mapOutputTracker: MapOutputTracker,
    val conf: SparkConf
) extends Logging {

  var isStopped = false

  def stop(): Unit = {

    if (!isStopped) {
      isStopped = true

      rpcEnv.shutdown()
      rpcEnv.awaitTermination()
    }
  }

  def awaitTermination(): Unit = {
    rpcEnv.awaitTermination()
  }
}

object SparkEnv extends Logging {

  private val env = new ThreadLocal[SparkEnv]
  private var lastSetSparkEnv: SparkEnv = _

  def set(e: SparkEnv): Unit = {
    lastSetSparkEnv = e
    env.set(e)
  }

  def get: SparkEnv = Option(env.get()).getOrElse(lastSetSparkEnv)

  def getThreadLocal: SparkEnv = env.get()

  def create(conf: SparkConf, isDriver: Boolean, isLocal: Boolean): SparkEnv = {

    def instantiateClass[T](propertyName: String, defaultClassName: String): T = {
      val name = conf.get(propertyName, defaultClassName)
      val cls = Class.forName(name, true, Utils.getContextOrSparkClassLoader)
      try {
        cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          cls.getConstructor().newInstance().asInstanceOf[T]
      }
    }

    val systemName = isDriver match {
      case true  => "driverSystem-rpc"
      case false => "executorSystem-rpc"
    }
    // netty rpcEnv listen
    val port = conf.get("spark.rpc.netty-port", "9990").toInt
    val rpcEnv = RpcEnv.create(systemName, "127.0.0.1", port, conf, 1)

    val serializer = instantiateClass[Serializer]("spark.serializer", 
      "xyz.sourcecodestudy.spark.serializer.JavaSerializer")

    val closureSerializer = instantiateClass[Serializer]("spark.closure.serializer", 
      "xyz.sourcecodestudy.spark.serializer.JavaSerializer")

    val shuffleFetcher = instantiateClass[ShuffleFetcher]("spark.shuffleFetcher", 
      "xyz.sourcecodestudy.spark.FileStoreShuffleFetcher")

    val mapOutputTracker = isDriver match {
      case true  => new MapOutputTrackerMaster(conf)
      case false => new MapOutputTrackerExecutor(conf)
    } 

    new SparkEnv(rpcEnv, serializer, closureSerializer, shuffleFetcher, mapOutputTracker, conf)
  }

}