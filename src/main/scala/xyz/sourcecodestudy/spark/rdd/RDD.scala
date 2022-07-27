package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag
import scala.collection.IterableOnce

import xyz.sourcecodestudy.spark.{TaskContext, Partition}

abstract class RDD[T: ClassTag](
    private val sc: SparkContext,
    private val deps: Seq[Dependency[_]]
  ) extends Serializable with Logging {

  // Construct for only one one-to-one dependency
  def this(oneParent: RDD[_]) = this(oneParent.context, List(new OneToOneDependency(oneParent)))

  def sparkContext: SparkContext = sc

  def context = sc
 
  def conf = sc.conf

  val id: Int = sc.newRddId()

  protected def getPartitions: Array[Partition]

  final def partitions: Array[Partition] = {
    // TODO: checkpoint
    getPartitions
  }

  protected def getDependencies: Seq[Dependency[_]] = deps

  final def dependencies: Seq[Dependency[_]] = {
    // TODO: checkpoint
    getDependencies
  }

  protected def firstParent[U: ClassTag] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  final def preferredLocations(split: Partition): Seq[String] = {
    // TODO: checkpoint
    getPreferredLocations(split)
  }

  val partitioner: Option[Partitioner] = None

  def compute(split: Partition, context: TaskContext): Iterator[T]

  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    compute(split, context)
  }

  // Transformations, return a new RDD

  def map[U: ClassTag](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))

  def flatMap[U: ClassTag](f: T => IterableOnce[U]): RDD[U] = new FlatMappedRDD(this, sc.clean(f))

  def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, sc.clean(f))

  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null): RDD[(K, Iterator[T])] = {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(p)
  }

  // Actions

  def foreach(f: T => Unit): Unit = {
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(f))
  }

  def foreachPartition(f: Iterator[T] => Unit): Unit = {
    sc.runJob(this, (iter: Iterator[T]) => f(iter))
  }

  def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(result: _*)
  }
  
}