package xyz.sourcecodestudy.spark

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._

import org.apache.logging.log4j.scala.Logging

class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging {

  def this() = this(true)

  private val setting = new HashMap[String, String]()

  if (loadDefaults) {
    for ((k, v) <- System.getProperties.asScala if k.startsWith("spark.")) {
      setting(k) = v
    }
  }

  def set(key: String, value: String): SparkConf = {
    (key, value) match {
      case (k: String, v: String) => setting(k) = v
      case _ => new NullPointerException("key / value is Null")
    }
    this
  }

  def setAll(settings: Iterable[(String, String)]) = {
    this.setting ++= setting
    this
  }

  def getOption(key: String): Option[String] = setting.get(key)

  def get(key: String, defaultValue: String): String = 
    setting.getOrElse(key, defaultValue)

  def contains(key: String): Boolean = setting.contains(key)

  override def clone: SparkConf = {
    new SparkConf(false).setAll(setting)
  }

  def toDebugString: String = {
    setting.toArray.sorted.map{case (k, v) => s"$k=$v"}.mkString("\n")
  }
}