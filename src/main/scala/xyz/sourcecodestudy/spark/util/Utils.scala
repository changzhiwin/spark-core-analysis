package xyz.sourcecodestudy.spark.util

import java.io._
import java.nio.channels.{FileChannel, WritableByteChannel}
import java.util.concurrent.{Executors, ThreadPoolExecutor}

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

  // scalastyle:off classforname
  /**
   * Preferred alternative to Class.forName(className), as well as
   * Class.forName(className, initialize, loader) with current thread's ContextClassLoader.
   */
  def classForName[C](
      className: String,
      initialize: Boolean = true,
      noSparkClassLoader: Boolean = false): Class[C] = {
    if (!noSparkClassLoader) {
      Class.forName(className, initialize, getContextOrSparkClassLoader).asInstanceOf[Class[C]]
    } else {
      Class.forName(className, initialize, Thread.currentThread().getContextClassLoader).
        asInstanceOf[Class[C]]
    }
  }

  /**
   * Copy all data from an InputStream to an OutputStream. NIO way of file stream to file stream
   * copying is disabled by default unless explicitly set transferToEnabled as true,
   * the parameter transferToEnabled should be configured by spark.file.transferTo = [true|false].
   */
  def copyStream(
      in: InputStream,
      out: OutputStream,
      closeStreams: Boolean = false,
      transferToEnabled: Boolean = false): Long = {
    try {
      (in, out) match {
        case (input: FileInputStream, output: FileOutputStream) if transferToEnabled =>
          // When both streams are File stream, use transferTo to improve copy performance.
          val inChannel = input.getChannel
          val outChannel = output.getChannel
          val size = inChannel.size()
          copyFileStreamNIO(inChannel, outChannel, 0, size)
          size
        case (input, output) =>
          var count = 0L
          val buf = new Array[Byte](8192)
          var n = 0
          while (n != -1) {
            n = input.read(buf)
            if (n != -1) {
              output.write(buf, 0, n)
              count += n
            }
          }
          count
      }
    } finally {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }

  def copyFileStreamNIO(
      input: FileChannel,
      output: WritableByteChannel,
      startPosition: Long,
      bytesToCopy: Long): Unit = {
    val outputInitialState = output match {
      case outputFileChannel: FileChannel =>
        Some((outputFileChannel.position(), outputFileChannel))
      case _ => None
    }
    var count = 0L
    // In case transferTo method transferred less data than we have required.
    while (count < bytesToCopy) {
      count += input.transferTo(count + startPosition, bytesToCopy - count, output)
    }
    assert(count == bytesToCopy,
      s"request to copy $bytesToCopy bytes, but actually copied $count bytes.")

    // Check the position after transferTo loop to see if it is in the right position and
    // give user information if not.
    // Position will not be increased to the expected length after calling transferTo in
    // kernel version 2.6.32, this issue can be seen in
    // https://bugs.openjdk.java.net/browse/JDK-7052359
    // This will lead to stream corruption issue when using sort-based shuffle (SPARK-3948).
    outputInitialState.foreach { case (initialPos, outputFileChannel) =>
      val finalPos = outputFileChannel.position()
      val expectedPos = initialPos + bytesToCopy
      assert(finalPos == expectedPos,
        s"""
           |Current position $finalPos do not equal to expected position $expectedPos
           |after transferTo, please check your kernel version to see if it is 2.6.32,
           |this is a kernel bug which will lead to unexpected behavior when using transferTo.
           |You can set spark.file.transferTo = false to disable this NIO feature.
         """.stripMargin)
    }
  }
    
}