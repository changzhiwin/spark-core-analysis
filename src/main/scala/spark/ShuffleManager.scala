package spark

import java.io._
import java.net.URL
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.{ArrayBuffer, HashMap}

import spark._

class ShuffleManager extends Logging {
  private var nextShuffleId = new AtomicLong(0)

  private var shuffleDir: File = null
  private var server: HttpServer = null
  private var serverUri: String = null

  initialize()

  private def initialize() {
    // TODO: localDir should be created by some mechanism common to Spark
    // so that it can be shared among shuffle, broadcast, etc
    val localDirRoot = System.getProperty("spark.local.dir", "/tmp")
    var tries = 0
    var foundLocalDir = false
    var localDir: File = null
    var localDirUuid: UUID = null
    while (!foundLocalDir && tries < 10) {
      tries += 1
      try {
        localDirUuid = UUID.randomUUID
        localDir = new File(localDirRoot, "spark-local-" + localDirUuid)
        if (!localDir.exists) {
          localDir.mkdirs()
          foundLocalDir = true
        }
      } catch {
        case e: Exception =>
          logWarning("Attempt " + tries + " to create local dir failed", e)
      }
    }
    if (!foundLocalDir) {
      logError("Failed 10 attempts to create local dir in " + localDirRoot)
      System.exit(1)
    }
    shuffleDir = new File(localDir, "shuffle")
    shuffleDir.mkdirs()
    logInfo("Shuffle dir: " + shuffleDir)

    // Add a shutdown hook to delete the local dir
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark local dir") {
      override def run() {
        Utils.deleteRecursively(localDir)
      }
    })
    
    val extServerPort = System.getProperty(
      "spark.localFileShuffle.external.server.port", "-1").toInt
    if (extServerPort != -1) {
      // We're using an external HTTP server; set URI relative to its root
      var extServerPath = System.getProperty(
        "spark.localFileShuffle.external.server.path", "")
      if (extServerPath != "" && !extServerPath.endsWith("/")) {
        extServerPath += "/"
      }
      serverUri = "http://%s:%d/%s/spark-local-%s".format(
        Utils.localIpAddress, extServerPort, extServerPath, localDirUuid)
    } else {
      // Create our own server
      server = new HttpServer(localDir)
      server.start()
      serverUri = server.uri
    }
    logInfo("Local URI: " + serverUri)
  }

  def stop() {
    if (server != null) {
      server.stop()
    }
  }

  def getOutputFile(shuffleId: Long, inputId: Int, outputId: Int): File = {
    val dir = new File(shuffleDir, shuffleId + "/" + inputId)
    dir.mkdirs()
    val file = new File(dir, "" + outputId)
    return file
  }

  def getServerUri(): String = {
    serverUri
  }

  def newShuffleId(): Long = {
    nextShuffleId.getAndIncrement()
  }
}
