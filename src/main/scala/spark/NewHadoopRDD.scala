package spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._

import java.util.Date
import java.text.SimpleDateFormat

class NewHadoopSplit(rddId: Int, val index: Int, @transient rawSplit: InputSplit with Writable)
  extends Split {
  
  val serializableHadoopSplit = new SerializableWritable(rawSplit)

  override def hashCode(): Int = (41 * (41 + rddId) + index).toInt
}

class NewHadoopRDD[K, V](
    sc: SparkContext,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K], valueClass: Class[V],
    @transient conf: Configuration)
  extends RDD[(K, V)](sc)
  with HadoopMapReduceUtil {
  
  private val serializableConf = new SerializableWritable(conf)

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient
  private val jobId = new JobID(jobtrackerId, id)

  @transient
  private val splits_ : Array[Split] = {
    val inputFormat = inputFormatClass.newInstance
    val jobContext = newJobContext(serializableConf.value, jobId)
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Split](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopSplit(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }

  override def splits = splits_

  override def compute(theSplit: Split) = new Iterator[(K, V)] {
    val split = theSplit.asInstanceOf[NewHadoopSplit]
    val conf = serializableConf.value
    val attemptId = new TaskAttemptID(jobtrackerId, id, true, split.index, 0)
    val context = newTaskAttemptContext(serializableConf.value, attemptId)
    val format = inputFormatClass.newInstance
    val reader = format.createRecordReader(split.serializableHadoopSplit.value, context)
    reader.initialize(split.serializableHadoopSplit.value, context)
   
    var havePair = false
    var finished = false

    override def hasNext: Boolean = {
      if (!finished && !havePair) {
        finished = !reader.nextKeyValue
        havePair = !finished
        if (finished) {
          reader.close
        }
      }
      !finished
    }

    override def next: (K, V) = {
      if (!hasNext) {
        throw new java.util.NoSuchElementException("End of stream")
      }
      havePair = false
      return (reader.getCurrentKey, reader.getCurrentValue)
    }
  }

  override def preferredLocations(split: Split) = {
    val theSplit = split.asInstanceOf[NewHadoopSplit]
    theSplit.serializableHadoopSplit.value.getLocations.filter(_ != "localhost")
  }

  override val dependencies: List[Dependency[_]] = Nil
}
