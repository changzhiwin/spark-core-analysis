package xyz.sourcecodestudy.spark.util

import java.util.concurrent.ThreadPoolExecutor
import org.apache.logging.log4j.scala.Logging
import com.google.common.util.concurrent.ThreadFactoryBuilder

object Utils extends Logging {

  def getSparkClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  private val daemonThreadFactoryBuilder: ThreadFactoryBuilder =
    new ThreadFactoryBuilder().setDaemon(true)

  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = daemonThreadFactoryBuilder.setNameFormat(prefix + "-d%").build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = daemonThreadFactoryBuilder.setNameFormat(prefix + "-d%").build()
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }
    
}