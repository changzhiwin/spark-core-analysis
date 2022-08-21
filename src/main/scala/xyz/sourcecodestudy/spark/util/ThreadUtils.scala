package xyz.sourcecodestudy.spark.util

import java.util.concurrent.{ThreadFactory, ThreadPoolExecutor, Executors, ExecutorService}

import scala.util.control.NonFatal
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

import com.google.common.util.concurrent.ThreadFactoryBuilder

object ThreadUtils {

  def sameThread: ExecutionContextExecutor = ???

  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(s"${prefix}-%d").build()
  }

  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadExecutor(threadFactory)
  }

}