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
    // 同一个shuffle rdd的写操作，独占访问
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
      // array判断不为null，说明之前是处理过了的；并且之前处理的数据因某些原因，变成脏数据，不能用；所以变更版本号
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
      // 线程间的同步，同一进程只允许一个线程取数据
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
          // 注意这里有直接返回！！！
          // 本进程中有线程在取的情况下，一直等待结果；其他线程标记完成后，直接返回结果
          return serverUris.get(shuffleId)
        } else {
          fetching += shuffleId
        }
      }
      // We won the race to fetch the output locs; do so
      // 上面没有提前返回，说明该线程是第一个希望取结果的
      logInfo("Doing the fetch; tracker actor = " + trackerActor)
      // 我理解这里是去master上询问，只会发生在worker上面；因为在master上运行本函数，locs不会为空的！！
      val fetched = (trackerActor !? GetMapOutputLocations(shuffleId)).asInstanceOf[Array[String]]
      // 在worker上serverUris变量获得数据的地方！！！
      serverUris.put(shuffleId, fetched)
      // 通知其他线程，已经取到结果了
      fetching.synchronized {
        fetching -= shuffleId
        fetching.notifyAll()
      }
      return fetched
    } else {
      return locs
    }
  }
  
  // 这个版本没有强调shuffle write/read的概念，而是受hadoop的影响使用map/reduce的术语
  def getMapOutputUri(serverUri: String, shuffleId: Int, mapId: Int, reduceId: Int): String = {
    "%s/shuffle/%s/%s/%s".format(serverUri, shuffleId, mapId, reduceId)
  }

  def stop() {
    trackerActor !? StopMapOutputTracker
    serverUris.clear()
    trackerActor = null
  }

  // Called on master to increment the generation number
  // 源作者写了注释，这个函数只在master上运行
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
  // 源作者写了注释，这个函数只在worker上运行；是来重置worker上tracker信息
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
