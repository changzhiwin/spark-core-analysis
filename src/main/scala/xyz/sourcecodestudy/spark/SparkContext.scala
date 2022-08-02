package xyz.sourcecodestudy.spark

import java.util.concurrent.atomic.AtomicInteger
//import java.util.{Properties, UUID}
//import java.util.UUID.randomUUID

import org.apache.logging.log4j.scala.Logging

import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.rdd.{RDD, ParallelCollectionRDD}
import xyz.sourcecodestudy.spark.scheduler.{TaskScheduler, TaskSchedulerImpl, DAGScheduler}
import xyz.sourcecodestudy.spark.scheduler.local.LocalBackend
import xyz.sourcecodestudy.spark.util.ClosureCleaner

class SparkContext(config: SparkConf) extends Logging {

  def this() = this(new SparkConf())

  val conf = config.clone()

  val master = conf.get("spark.master", "local-master")
  
  val isLocal = master.startsWith("local")

  val env = SparkEnv.create(
    conf,
    isDriver = true,
    isLocal = isLocal)
  SparkEnv.set(env)

  // Scheduler
  val taskScheduler = SparkContext.createTaskScheduler(this, master)

  val dagScheduler: DAGScheduler = ???

  taskScheduler.start()

  def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner.clean(f)
    f
  }

  // Methods for creating RDDs

  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }

  /**
   * Run a function on a given set of partitions in an RDD and pass the results to the given
   * handler function
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit): Unit = {

    if (dagScheduler == null) {
      throw new SparkException("DAG scheduler not Found")
    }
    
    val start = System.nanoTime
    dagScheduler.runJob(rdd, func, partitions, allowLocal, resultHandler)
    logger.info(s"Job finished: rdd(${rdd.id}, ${partitions}), took ${(System.nanoTime - start) / 1e9} s")
  }

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T, U](rdd, func, partitions, allowLocal, (index, res) => results(index) = res)
    results
  }

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, (context: TaskContext, iter: Iterator[T]) => func(iter), 0 until rdd.partitions.size, false)
  }

  // Process Auto ID
  private val nextShuffledId = new AtomicInteger(0)
  private val nextRddId = new AtomicInteger(0)

  def newShuffledId(): Int = nextShuffledId.getAndIncrement()
  def newRddId(): Int = nextRddId.getAndIncrement()



  def defaultParallelism: Int = taskScheduler.defaultParallelism()

  def version = SparkContext.SPARK_VERSION
}

object SparkContext extends Logging {
  val SPARK_VERSION = "1.0.0-xyz"

  private def createTaskScheduler(sc: SparkContext, master: String): TaskScheduler = {
    // Regular expression used for local[N] and local[*] master formats
    val LOCAL_N_REGEX = """local\[([0-9\*]+)\]""".r
    // Regular expression for connecting to Spark deploy clusters
    // val SPARK_REGEX = """spark://(.*)""".r

    // When running locally, don't try to re-execute tasks on failure.
    val MAX_LOCAL_TASK_FAILURES = 1

    master match {
      case "local" =>
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalBackend(scheduler, 1)
        scheduler.initialize(backend)
        scheduler
      case LOCAL_N_REGEX(threads) =>
        def localCpuCount = Runtime.getRuntime.availableProcessors()
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalBackend(scheduler, threadCount)
        scheduler.initialize(backend)
        scheduler
      //case SPARK_REGEX(sparkUrl) =>
        //TODO
      case _ => 
        throw new SparkException(s"Not support master URL: ${master}")
    }
  }
}

/**
 * A class encapsulating how to convert some type T to Writable. It stores both the Writable class
 * corresponding to T (e.g. IntWritable for Int) and a function for doing the conversion.
 * The getter for the writable class takes a ClassTag[T] in case this is a generic object
 * that doesn't know the type of T when it is created. This sounds strange but is necessary to
 * support converting subclasses of Writable to themselves (writableWritableConverter).
 */
