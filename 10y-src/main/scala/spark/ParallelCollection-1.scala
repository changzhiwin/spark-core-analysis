package spark

import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer

class ParallelCollectionSplit[T: ClassManifest](
    val rddId: Long,
    val slice: Int,
    values: Seq[T])
  extends Split with Serializable {
  
  def iterator(): Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParallelCollectionSplit[_] => (this.rddId == that.rddId && this.slice == that.slice)
    case _ => false
  }

  override val index = slice
}

// SparkContext里面直接使用new ParallelCollection
class ParallelCollection[T: ClassManifest](
    sc: SparkContext, 
    @transient data: Seq[T],
    numSlices: Int)
  extends RDD[T](sc) {
  // TODO: Right now, each split sends along its full data, even if later down the RDD chain it gets
  // cached. It might be worthwhile to write the data to a file in the DFS and read it in the split
  // instead.

  @transient
  val splits_ = {
    // 平均拆分，得到数组
    val slices = ParallelCollection.slice(data, numSlices).toArray
    // 创建每个分片，id是从RDD继承来的 = sc.newRddId()
    slices.indices.map(i => new ParallelCollectionSplit(id, i, slices(i))).toArray
  }

  override def splits = splits_.asInstanceOf[Array[Split]]

  override def compute(s: Split) = s.asInstanceOf[ParallelCollectionSplit[T]].iterator
  
  override def preferredLocations(s: Split): Seq[String] = Nil
  
  override val dependencies: List[Dependency[_]] = Nil
}

private object ParallelCollection {
  /**
   * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
   * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes 
   * it efficient to run Spark over RDDs representing large sets of numbers.
   */
  // 把传入的集合，平均拆分成numSlice份，实现比较简单
  def slice[T: ClassManifest](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of slices required")
    }
    seq match {
      case r: Range.Inclusive => {
        val sign = if (r.step < 0) {
          -1 
        } else {
          1
        }
        slice(new Range(
            r.start, r.end + sign, r.step).asInstanceOf[Seq[T]], numSlices)
      }
      case r: Range => {
        (0 until numSlices).map(i => {
          val start = ((i * r.length.toLong) / numSlices).toInt
          val end = (((i+1) * r.length.toLong) / numSlices).toInt
          new Range(r.start + start * r.step, r.start + end * r.step, r.step)
        }).asInstanceOf[Seq[Seq[T]]]
      }
      case nr: NumericRange[_] => { // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        val sliceSize = (nr.size + numSlices - 1) / numSlices // Round up to catch everything
        var r = nr
        for (i <- 0 until numSlices) {
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices
      }
      case _ => {
        val array = seq.toArray  // To prevent O(n^2) operations for List etc
        (0 until numSlices).map(i => {
          val start = ((i * array.length.toLong) / numSlices).toInt
          val end = (((i+1) * array.length.toLong) / numSlices).toInt
          array.slice(start, end).toSeq
        })
      }
    }
  }
}
