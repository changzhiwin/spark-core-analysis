package xyz.sourcecodestudy.spark

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.util.Utils

class SparkEnv(
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val conf: SparkConf
) extends Logging {}

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

    val serializer = instantiateClass[Serializer]("spark.serializer", 
      "xyz.sourcecodestudy.spark.serializer.JavaSerializer")

    val closureSerializer = instantiateClass[Serializer]("spark.closure.serializer", 
      "xyz.sourcecodestudy.spark.serializer.JavaSerializer")

    new SparkEnv(serializer, closureSerializer, conf)
  }

}