package xyz.sourcecodestudy.spark

import java.util.concurrent.ConcurrentHashMap

//import scala.collection.mutable.{HashSet}

import org.apache.logging.log4j.scala.Logging

abstract class MapOutputTracker(conf: SparkConf) extends Logging {

  // 使用String 简单实现MapStatus，shuffleId -> data[String]
  protected val mapStatuses = new ConcurrentHashMap[Int, Array[String]]

  // private val fetching = new HashSet[Int]

  def unregisterShuffle(shuffleId: Int): Unit = {
    mapStatuses.remove(shuffleId)
  }

  def getServerStatuses(shuffleId: Int, reduceId: Int): Array[String] = {
    val statuses = mapStatuses.get(shuffleId)
    statuses
    // 下面的实现是集群版本，先忽略
    /*
    if (statuses == null ) {
      logger.warn(s"Don't have map outputs for shuffle ${shuffleId}")
      fetching.synchronized {
        if (fetching.contains(shuffleId)) {
          while (fetching.contains(shuffleId)) {
            try {
              fetching.wait()
            } catch {
              case _ =>
            }
          }

          mapStatuses.get(shuffleId)
        } else {
          fetching += shuffleId
        }
      }

      fetched = mapStatuses.get(shuffleId)
      fetching.synchronized {
        fetching -= shuffleId
        fetching.notifyAll()
      }
      fetched
    } else {
      statuses
    }
    */
  }

  def getMapOutputStatuses(shuffleId: Int): Array[String] = {
    mapStatuses.get(shuffleId)
  }

  def stop(): Unit
}

class MapOutputTrackerMaster(conf: SparkConf) extends MapOutputTracker(conf) {

  def registerShuffle(shuffleId: Int, numMaps: Int): Unit = {
    if (mapStatuses.get(shuffleId) != null) {
      throw new IllegalArgumentException(s"Shuffle ID ${shuffleId} registered twice")
    }
    mapStatuses.put(shuffleId, new Array[String](numMaps))
  }

  def registerMapOutput(shuffleId: Int, mapId: Int, mapState: String): Unit = {
    val array = mapStatuses.get(shuffleId)
    array.synchronized {
      array(mapId) = mapState
    }
  }

  def registerMapOutputs(shuffleId: Int, locs: Array[String]): Unit = {
    mapStatuses.put(shuffleId, Array[String]() ++ locs)
  }

  def containsShuffle(shuffleId: Int): Boolean = {
    mapStatuses.contains(shuffleId)
  }

  def stop(): Unit = {}
}

class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {
  def stop(): Unit = {}
}