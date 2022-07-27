package xyz.sourcecodestudy.spark.util

import org.apache.logging.log4j.scala.Logging

object Utils extends Logging {

  def getSparkClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
    
}