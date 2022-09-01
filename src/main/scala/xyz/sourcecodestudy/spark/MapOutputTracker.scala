package xyz.sourcecodestudy.spark

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.{HashSet}

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.executor.CoarseGrainedExecutorBackend

abstract class MapOutputTracker(conf: SparkConf) extends Logging {

  def role: String

  // 使用String 简单实现MapStatus，shuffleId -> data[String]
  protected val mapStatuses = new ConcurrentHashMap[Int, Array[String]]

  def unregisterShuffle(shuffleId: Int): Unit = {
    mapStatuses.remove(shuffleId)
  }

  def getServerStatuses(shuffleId: Int, reduceId: Int): Array[String] = {
    val statuses = mapStatuses.get(shuffleId)
    statuses
  }

  def getMapOutputStatuses(shuffleId: Int): Array[String] = {
    mapStatuses.get(shuffleId)
  }

  def containsShuffle(shuffleId: Int): Boolean = {
    mapStatuses.containsKey(shuffleId)
  }

  def registerMapOutputs(shuffleId: Int, locs: Array[String]): Unit = {
    mapStatuses.put(shuffleId, Array[String]() ++ locs)
  }

  def registerShuffle(shuffleId: Int, numMaps: Int): Unit = throw new UnsupportedOperationException()

  def registerMapOutput(shuffleId: Int, mapId: Int, mapState: String): Unit = throw new UnsupportedOperationException()

  def stop(): Unit = throw new UnsupportedOperationException()
}

class MapOutputTrackerMaster(conf: SparkConf) extends MapOutputTracker(conf) {

  override def role: String = "Master"

  override def registerShuffle(shuffleId: Int, numMaps: Int): Unit = {
    if (mapStatuses.get(shuffleId) != null) {
      throw new IllegalArgumentException(s"Shuffle ID ${shuffleId} registered twice")
    }
    mapStatuses.put(shuffleId, new Array[String](numMaps))
  }

  override def registerMapOutput(shuffleId: Int, mapId: Int, mapState: String): Unit = {
    val array = mapStatuses.get(shuffleId)
    array.synchronized {
      array(mapId) = mapState
    }
  }
}

class MapOutputTrackerExecutor(conf: SparkConf) extends MapOutputTracker(conf) {

  var executorBackendOpt: Option[CoarseGrainedExecutorBackend] = None

  def setBackend(backend: CoarseGrainedExecutorBackend): Unit = {
    executorBackendOpt = Some(backend)
  }

  override def role: String = "Executor"

  private val fetching = new HashSet[Int]

  override def getServerStatuses(shuffleId: Int, reduceId: Int): Array[String] = {

    if (containsShuffle(shuffleId)) {
      mapStatuses.get(shuffleId)
    } else {
      assert(executorBackendOpt != None, "Executor need request mapOut, but backend is None.")

      // Fetch from remote driver
      fetching.synchronized {
        if (fetching.contains(shuffleId)) {
          while (fetching.contains(shuffleId)) {
            try {
              fetching.wait()
            } catch {
              case _ : Throwable =>
            }
          }

          mapStatuses.get(shuffleId)
        } else {
          fetching += shuffleId
        }
      }

      // Request remote driver, and wait
      val statuses = executorBackendOpt.get.fetchMapOutStatuses(shuffleId, reduceId)
      registerMapOutputs(shuffleId, statuses)

      fetching.synchronized {
        fetching -= shuffleId
        fetching.notifyAll()
      }
      statuses
    }
  }
}