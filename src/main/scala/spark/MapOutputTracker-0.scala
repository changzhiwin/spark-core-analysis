package spark

import java.util.concurrent.ConcurrentHashMap

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.collection.mutable.HashSet

sealed trait MapOutputTrackerMessage
case class GetMapOutputLocations(shuffleId: Int) extends MapOutputTrackerMessage 
case object StopMapOutputTracker extends MapOutputTrackerMessage

class MapOutputTrackerActor(serverUris: ConcurrentHashMap[Int, Array[String]])
extends DaemonActor with Logging {
  def act() {
    val port = System.getProperty("spark.master.port").toInt
    RemoteActor.alive(port)
    RemoteActor.register('MapOutputTracker, self)
    logInfo("Registered actor on port " + port)
    
    loop {
      react {
        case GetMapOutputLocations(shuffleId: Int) =>
          logInfo("Asked to get map output locations for shuffle " + shuffleId)
          reply(serverUris.get(shuffleId))
          
        case StopMapOutputTracker =>
          reply('OK)
          exit()
      }
    }
  }
}

class MapOutputTracker(isMaster: Boolean) extends Logging {
  var trackerActor: AbstractActor = null

  // 好像是线程共享的，需要再理解理解TODO
  private var serverUris = new ConcurrentHashMap[Int, Array[String]]

  // Incremented every time a fetch fails so that client nodes know to clear
  // their cache of map output locations if this happens.
  private var generation: Long = 0
  private var generationLock = new java.lang.Object
  
  if (isMaster) {
    val tracker = new MapOutputTrackerActor(serverUris)
    tracker.start()
    trackerActor = tracker
  } else {
    val host = System.getProperty("spark.master.host")
    val port = System.getProperty("spark.master.port").toInt
    trackerActor = RemoteActor.select(Node(host, port), 'MapOutputTracker)
  }

  // 注册ShuffledRDD的数据地址
  // 在DAGScheduler里面newStage方法里调用：shuffleId需要被shuffle write的rdd
  // 疑问：numMaps应该是被shuffle write的rdd的分区个数才对吧？但看newStage调用的参数有点没明白TODO
  // 回答：numMaps确实是shuffleId对应rdd的分区个数，new Array[String](numMaps)这个数组存放对应分区数据存放地址
  // 特别需要注意的是这个数组下标和分区是一一映射，使得在fetch的时候可以直接使用下标作为分区标识！！！
  def registerShuffle(shuffleId: Int, numMaps: Int) {
    if (serverUris.get(shuffleId) != null) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
    serverUris.put(shuffleId, new Array[String](numMaps))
  }
  
  // 注册一个map处理后的数据存放位置
  def registerMapOutput(shuffleId: Int, mapId: Int, serverUri: String) {
    var array = serverUris.get(shuffleId)
    array.synchronized {
      array(mapId) = serverUri
    }
  }
  
  def registerMapOutputs(shuffleId: Int, locs: Array[String]) {
    serverUris.put(shuffleId, Array[String]() ++ locs)
  }

  def unregisterMapOutput(shuffleId: Int, mapId: Int, serverUri: String) {
    var array = serverUris.get(shuffleId)
    if (array != null) {
      array.synchronized {
        if (array(mapId) == serverUri) {
          array(mapId) = null
        }
      }
      // 可能是考虑过期的机制，类似版本号
      incrementGeneration()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }
  
  // Remembers which map output locations are currently being fetched on a worker
  // 记录是否有其他worker在获取，避免竞争
  val fetching = new HashSet[Int]
  
  // Called on possibly remote nodes to get the server URIs for a given shuffle
  //获取shuffleRDD的数据存放地址列表；使用同步机制这块不是太明白，TDDO
  def getServerUris(shuffleId: Int): Array[String] = {
    val locs = serverUris.get(shuffleId)
    if (locs == null) {
      logInfo("Don't have map outputs for " + shuffleId + ", fetching them")
      fetching.synchronized {
        if (fetching.contains(shuffleId)) {
          // Someone else is fetching it; wait for them to be done
          while (fetching.contains(shuffleId)) {
            try {
              fetching.wait()
            } catch {
              case _ =>
            }
          }
          return serverUris.get(shuffleId)
        } else {
          fetching += shuffleId
        }
      }
      // We won the race to fetch the output locs; do so
      logInfo("Doing the fetch; tracker actor = " + trackerActor)
      val fetched = (trackerActor !? GetMapOutputLocations(shuffleId)).asInstanceOf[Array[String]]
      serverUris.put(shuffleId, fetched)
      fetching.synchronized {
        fetching -= shuffleId
        fetching.notifyAll()
      }
      return fetched
    } else {
      return locs
    }
  }
  
  def getMapOutputUri(serverUri: String, shuffleId: Int, mapId: Int, reduceId: Int): String = {
    "%s/shuffle/%s/%s/%s".format(serverUri, shuffleId, mapId, reduceId)
  }

  def stop() {
    trackerActor !? StopMapOutputTracker
    serverUris.clear()
    trackerActor = null
  }

  // Called on master to increment the generation number
  def incrementGeneration() {
    generationLock.synchronized {
      generation += 1
    }
  }

  // Called on master or workers to get current generation number
  def getGeneration: Long = {
    generationLock.synchronized {
      return generation
    }
  }

  // Called on workers to update the generation number, potentially clearing old outputs
  // because of a fetch failure. (Each Mesos task calls this with the latest generation
  // number on the master at the time it was created.)
  def updateGeneration(newGen: Long) {
    generationLock.synchronized {
      if (newGen > generation) {
        logInfo("Updating generation to " + newGen + " and clearing cache")
        serverUris = new ConcurrentHashMap[Int, Array[String]]
        generation = newGen
      }
    }
  }
}
