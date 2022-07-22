package spark

import java.io._
import java.net.InetAddress
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import scala.collection.mutable.ArrayBuffer
import java.util.{Locale, UUID, Random}

/**
 * Various utility methods used by Spark.
 */
object Utils {
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close
    return bos.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    return ois.readObject.asInstanceOf[T]
  }

  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    return ois.readObject.asInstanceOf[T]
  }

  def isAlpha(c: Char): Boolean = {
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  }

  def splitWords(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var i = 0
    while (i < s.length) {
      var j = i
      while (j < s.length && isAlpha(s.charAt(j))) {
        j += 1
      }
      if (j > i) {
        buf += s.substring(i, j);
      }
      i = j
      while (i < s.length && !isAlpha(s.charAt(i))) {
        i += 1
      }
    }
    return buf
  }

  // Create a temporary directory inside the given parent directory
  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory after " + maxAttempts + 
            " attempts!")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: IOException => ; }
    }
    // Add a shutdown hook to delete the temp dir when the JVM exits
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dir " + dir) {
      override def run() {
        Utils.deleteRecursively(dir)
      }
    })
    return dir
  }

  // Copy all data from an InputStream to an OutputStream
  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false)
  {
    val buf = new Array[Byte](8192)
    var n = 0
    while (n != -1) {
      n = in.read(buf)
      if (n != -1) {
        out.write(buf, 0, n)
      }
    }
    if (closeStreams) {
      in.close()
      out.close()
    }
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
   */
  def randomize[T: ClassManifest](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
   */
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   */
  def localIpAddress(): String = InetAddress.getLocalHost.getHostAddress
  
  /**
   * Returns a standard ThreadFactory except all threads are daemons.
   */
  private def newDaemonThreadFactory: ThreadFactory = {
    new ThreadFactory {
      def newThread(r: Runnable): Thread = {
        var t = Executors.defaultThreadFactory.newThread (r)
        t.setDaemon (true)
        return t
      }
    }
  }

  /**
   * Wrapper over newCachedThreadPool.
   */
  def newDaemonCachedThreadPool(): ThreadPoolExecutor = {
    var threadPool = Executors.newCachedThreadPool.asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory (newDaemonThreadFactory)

    return threadPool
  }

  /**
   * Wrapper over newFixedThreadPool.
   */
  def newDaemonFixedThreadPool(nThreads: Int): ThreadPoolExecutor = {
    var threadPool = Executors.newFixedThreadPool(nThreads).asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory(newDaemonThreadFactory)

    return threadPool
  }

  /**
   * Get the local machine's hostname.
   */
  def localHostName(): String = InetAddress.getLocalHost.getHostName

  /**
   * Get current host
   */
  def getHost = System.getProperty("spark.hostname", localHostName())

  /**
   * Delete a file or directory and its contents recursively.
   */
  def deleteRecursively(file: File) {
    if (file.isDirectory) {
      for (child <- file.listFiles()) {
        deleteRecursively(child)
      }
    }
    if (!file.delete()) {
      throw new IOException("Failed to delete: " + file)
    }
  }

  /**
   * Use unit suffixes (Byte, Kilobyte, Megabyte, Gigabyte, Terabyte and
   * Petabyte) in order to reduce the number of digits to four or less. For
   * example, 4,000,000 is returned as 4MB.
   */
  def memoryBytesToString(size: Long): String = {
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f%s".formatLocal(Locale.US, value, unit)
  }
}
