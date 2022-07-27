package xyz.sourcecodestudy.spark.rdd

import scala.reflect.ClassTag
import xyz.sourcecodestudy.spark.Partition

class ParallelCollectionPartition[T: ClassTag](
    val rddId: Long,
    val slice: Int,
    val values: Seq[T])
  extends Partition with Serializable {
  
  def iterator: Iterator[T] = values.iterator

  override def index: Int = slice

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParallelCollectionPartition[_] =>
      this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }
}

class ParallelCollectionRDD[T: ClassTag](
    sc: SparkContext,
    data: Seq[T],
    numSlices: Int,
    locationPrefs: Map[Int, Seq[String]])
  extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    split.asInstanceOf[ParallelCollectionPartition[T]].iterator
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    locationPrefs.getOrElse(split.index, Nil)
  }
}

private object ParallelCollectionRDD {
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of slices required")
    }

    seq match {
      case r: Range.Inclusive => {
        val sign = if (r.step < 0) -1 else 1
        slice(new Range(r.start, r.end + sign, r.step).asInstanceOf[Seq[T]], numSlices)
      }
      case r: Range => {
        val total = r.length
        (0 until numSlices).map(i => {
          val start = ((i * total) / numSlices).toInt
          val end = (((i + 1) * total) / numSlices).toInt
          new Range(r.start + start * r.step, r.start + end * r.step, r.step)
        }).asInstanceOf[Seq[Seq[T]]]
      }
      case nr: NumericRange[_] => {
        val sliceSize = (nr.size + numSlices - 1) / numSlices
        var r = nr
        (0 until numSlices).map( i => {
          val part = r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
          part
        })
      }
      case _ => {
        val total = seq.size
        val step = total / numSlices
        var modLeft = total % numSlices
        var start = 0

        (0 until numSlices).map( i => {
          val mod = if (modLeft > 0) { modeLeft -= 1; 1 } else 0
          val end = start + step + mod
          val part = seq.slice(start, end)
          start = end
          part
        })
      }
    }
  }
}