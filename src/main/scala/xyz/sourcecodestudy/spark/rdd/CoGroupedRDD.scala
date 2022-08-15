package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer

import xyz.sourcecodestudy.spark.{SparkEnv, Aggregator, Partition, Partitioner, TaskContext}
import xyz.sourcecodestudy.spark.{Dependency, OneToOneDependency, ShuffleDependency}
import xyz.sourcecodestudy.spark.serializer.Serializer

case class NarrowCoGroupSplitDep(
    rdd: RDD[_],
    splitIndex: Int,
    var split: Partition) extends Serializable {

  /* ???
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }*/
}

class CoGroupedPartition(override val index: Int, 
    val narrowDeps: Array[Option[NarrowCoGroupSplitDep]]) extends Partition with Serializable {

  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = super.equals(other)
}

class CoGroupedRDD[K: ClassTag](val rdds: Seq[RDD[(K, _)]], part: Partitioner)
    extends RDD[(K, Array[Iterable[_]])](rdds.head.context, Nil) {

  // 原版使用CompactBuffer，是一个优化版本的ArrayBuffer，为尽量少的依赖，直接用ArrayBuffer
  private type CoGroup = ArrayBuffer[Any]
  private type CoGroupValue = (Any, Int)
  private type CoGroupCombiner = Array[CoGroup]  

  // 设置实例后，会导致Task序列化失败！！！TODO
  private var serializer: Serializer = _
  //Serializer.getSerializer(serializer)

  override val partitioner = Some(part)

  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_] =>
      if (rdd.partitioner == Some(part)) {
        new OneToOneDependency(rdd)
      } else {
        new ShuffleDependency[K, Any](rdd.asInstanceOf[RDD[(K, Any)]], part, serializer)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- array.indices) {
      array(i) = new CoGroupedPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _] =>
            None
          case _ =>
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    val split = s.asInstanceOf[CoGroupedPartition]
    val numRdds = dependencies.length

    val rddIterators = new ArrayBuffer[(Iterator[(K, Any)], Int)]
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
      case oneToOne: OneToOneDependency[(K, Any)] @unchecked =>
        val dependencyPartition = split.narrowDeps(depNum).get.split
        val it = oneToOne.rdd.iterator(dependencyPartition, context)
        rddIterators += ((it, depNum))
      case shuffle: ShuffleDependency[_, _] =>
        // read shuffle data
        val ser = Serializer.getSerializer(serializer)
        val it = SparkEnv.get.shuffleFetcher.fetch[(K, Any)](shuffle.shuffleId, split.index, context, ser)
        rddIterators += ((it, depNum))
    }

    // 转换成Iterable[(K, CoGroupValue)]
    val allKv = for {
      (it, depNum) <- rddIterators
      pair <- it
    } yield (pair._1, new CoGroupValue(pair._2, depNum))
    /*
    val its = rddIterators.flatMap { case (it, depNum) =>
      it.map(pair => (pair._1, new CoGroupValue(pair._2, depNum)))//.asInstanceOf[Iterable[(K, CoGroupValue)]]
    }*/

    // 这里使用了纯内存的聚合
    val agg = createAggretor(numRdds)
    val ret = agg.combineValuesByKey(allKv.iterator, context)
    ret.asInstanceOf[Iterator[(K, Array[Iterable[_]])]]
  }

  /**
    * 这个合并是把每个key，映射成了一个数组，长度是cogroup的rdd个数
    * 不同rdd的元素存放不同下标，这步得到的结果是：
    * +-------------------------------
    * |        |  rdd0[v0, v3]
    * |  key0  |  rdd1[v1, v2]
    * |        |  rdd2[v0]
    * +-------------------------------
    * |        |  rdd0[v3, v4]
    * |  key1  |  rdd1[v0, v1]
    * |        |  rdd2[v5]
    * +------------------------------
    * 完成了两个关键动作：
    * 1，把值给combine了
    * 2，同时区分开，归属不同rdd
    */
  private def createAggretor(numRdds: Int): Aggregator[K, CoGroupValue, CoGroupCombiner] = {
    val createCombiner: (CoGroupValue => CoGroupCombiner) = value => {
      val newCombiner = Array.fill(numRdds)(new CoGroup)
      newCombiner(value._2) += value._1
      newCombiner
    }
    
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner = (combiner, value) => {
      combiner(value._2) += value._1
      combiner
    }

    val mergeCombiners: (CoGroupCombiner, CoGroupCombiner) => CoGroupCombiner = (combiner1, combiner2) => {
      var depNum = 0
      while (depNum < numRdds) {
        combiner1(depNum) ++= combiner2(depNum)
        depNum += 1
      }
      combiner1
    }

    new Aggregator[K, CoGroupValue, CoGroupCombiner](createCombiner, mergeValue, mergeCombiners)
  }
}