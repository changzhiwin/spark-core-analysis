package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.{SparkEnv, TaskContext, Partition, Partitioner, Dependency, ShuffleDependency}
import xyz.sourcecodestudy.spark.serializer.Serializer

class ShuffledRDDPartition(idx: Int) extends Partition {
  override val index = idx
  override def hashCode(): Int = idx
}

class ShuffledRDD[K, V, P: ClassTag](val prev: RDD[P], part: Partitioner) 
    extends RDD[P](prev.context, Seq(new ShuffleDependency(prev.asInstanceOf[RDD[(K, V)]], part, null))) {
    //extends RDD[P](prev.context, Nil) {
  
  private var serializer: Serializer = _

  def setSerializer(serializer: Serializer): ShuffledRDD[K, V, P] = {
    this.serializer = serializer
    this
  }

  // 为啥每次要new？
  //override protected def getDependencies: Seq[Dependency[_]] = {
    //List(new ShuffleDependency[K, V](prev.asInstanceOf[RDD[(K, V)]], part, serializer))
  //}

  override val partitioner = Some(part)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[P] = {
    val shuffleId = dependencies.head.asInstanceOf[ShuffleDependency[K, V]].shuffleId

    // 调用object上的方法
    val ser = Serializer.getSerializer(serializer)
    // 根据约定(shuffleId, mapId, reduceId)的文件位置，获取数据
    SparkEnv.get.shuffleFetcher.fetch[P](shuffleId, split.index, context, ser)
  }

}