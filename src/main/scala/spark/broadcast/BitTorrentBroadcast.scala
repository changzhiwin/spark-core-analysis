package spark.broadcast

import java.io._
import java.net._
import java.util.{BitSet, Comparator, Random, Timer, TimerTask, UUID}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ListBuffer, Map, Set}
import scala.math

import spark._

class BitTorrentBroadcast[T](@transient var value_ : T, isLocal: Boolean)
extends Broadcast[T] with Logging with Serializable {

  def value = value_

  BitTorrentBroadcast.synchronized {
    BitTorrentBroadcast.values.put(uuid, 0, value_)
  }

  @transient var arrayOfBlocks: Array[BroadcastBlock] = null
  @transient var hasBlocksBitVector: BitSet = null
  @transient var numCopiesSent: Array[Int] = null
  @transient var totalBytes = -1
  @transient var totalBlocks = -1
  @transient var hasBlocks = new AtomicInteger(0)
  // CHANGED: BlockSize in the Broadcast object is expected to change over time
  @transient var blockSize = Broadcast.BlockSize

  // Used ONLY by Master to track how many unique blocks have been sent out
  @transient var sentBlocks = new AtomicInteger(0)

  @transient var listenPortLock = new Object
  @transient var guidePortLock = new Object
  @transient var totalBlocksLock = new Object

  @transient var listOfSources = ListBuffer[SourceInfo]()

  @transient var serveMR: ServeMultipleRequests = null

  // Used only in Master
  @transient var guideMR: GuideMultipleRequests = null

  // Used only in Workers
  @transient var ttGuide: TalkToGuide = null

  @transient var rxSpeeds = new SpeedTracker
  @transient var txSpeeds = new SpeedTracker

  @transient var hostAddress = Utils.localIpAddress
  @transient var listenPort = -1
  @transient var guidePort = -1

  @transient var hasCopyInHDFS = false
  @transient var stopBroadcast = false

  // Must call this after all the variables have been created/initialized
  if (!isLocal) {
    sendBroadcast
  }

  def sendBroadcast(): Unit = {
    logInfo("Local host address: " + hostAddress)

    // Store a persistent copy in HDFS
    // TODO: Turned OFF for now. Related to persistence
    // val out = new ObjectOutputStream(BroadcastCH.openFileForWriting(uuid))
    // out.writeObject(value_)
    // out.close()
    // FIXME: Fix this at some point
    hasCopyInHDFS = true

    // Create a variableInfo object and store it in valueInfos
    var variableInfo = Broadcast.blockifyObject(value_)

    // Prepare the value being broadcasted
    // TODO: Refactoring and clean-up required here
    arrayOfBlocks = variableInfo.arrayOfBlocks
    totalBytes = variableInfo.totalBytes
    totalBlocks = variableInfo.totalBlocks
    hasBlocks.set(variableInfo.totalBlocks)

    // Guide has all the blocks
    hasBlocksBitVector = new BitSet(totalBlocks)
    hasBlocksBitVector.set(0, totalBlocks)

    // Guide still hasn't sent any block
    numCopiesSent = new Array[Int](totalBlocks)

    guideMR = new GuideMultipleRequests
    guideMR.setDaemon(true)
    guideMR.start()
    logInfo("GuideMultipleRequests started...")

    // Must always come AFTER guideMR is created
    while (guidePort == -1) {
      guidePortLock.synchronized {
        guidePortLock.wait
      }
    }

    serveMR = new ServeMultipleRequests
    serveMR.setDaemon(true)
    serveMR.start()
    logInfo("ServeMultipleRequests started...")

    // Must always come AFTER serveMR is created
    while (listenPort == -1) {
      listenPortLock.synchronized {
        listenPortLock.wait
      }
    }

    // Must always come AFTER listenPort is created
    val masterSource =
      SourceInfo(hostAddress, listenPort, totalBlocks, totalBytes, blockSize)
    hasBlocksBitVector.synchronized {
      masterSource.hasBlocksBitVector = hasBlocksBitVector
    }

    // In the beginning, this is the only known source to Guide
    listOfSources += masterSource

    // Register with the Tracker
    registerBroadcast(uuid,
      SourceInfo(hostAddress, guidePort, totalBlocks, totalBytes, blockSize))
  }

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject
    BitTorrentBroadcast.synchronized {
      val cachedVal = BitTorrentBroadcast.values.get(uuid, 0)

      if (cachedVal != null) {
        value_ = cachedVal.asInstanceOf[T]
      } else {
        // Only the first worker in a node can ever be inside this 'else'
        initializeWorkerVariables

        logInfo("Local host address: " + hostAddress)

        // Start local ServeMultipleRequests thread first
        serveMR = new ServeMultipleRequests
        serveMR.setDaemon(true)
        serveMR.start()
        logInfo("ServeMultipleRequests started...")

        val start = System.nanoTime

        val receptionSucceeded = receiveBroadcast(uuid)
        // If does not succeed, then get from HDFS copy
        if (receptionSucceeded) {
          value_ = Broadcast.unBlockifyObject[T](arrayOfBlocks, totalBytes, totalBlocks)
          BitTorrentBroadcast.values.put(uuid, 0, value_)
        }  else {
          // TODO: This part won't work, cause HDFS writing is turned OFF
          val fileIn = new ObjectInputStream(DfsBroadcast.openFileForReading(uuid))
          value_ = fileIn.readObject.asInstanceOf[T]
          BitTorrentBroadcast.values.put(uuid, 0, value_)
          fileIn.close()
        }

        val time = (System.nanoTime - start) / 1e9
        logInfo("Reading Broadcasted variable " + uuid + " took " + time + " s")
      }
    }
  }

  // Initialize variables in the worker node. Master sends everything as 0/null
  private def initializeWorkerVariables: Unit = {
    arrayOfBlocks = null
    hasBlocksBitVector = null
    numCopiesSent = null
    totalBytes = -1
    totalBlocks = -1
    hasBlocks = new AtomicInteger(0)
    blockSize = -1

    listenPortLock = new Object
    totalBlocksLock = new Object

    serveMR = null
    ttGuide = null

    rxSpeeds = new SpeedTracker
    txSpeeds = new SpeedTracker

    hostAddress = Utils.localIpAddress
    listenPort = -1

    listOfSources = ListBuffer[SourceInfo]()

    stopBroadcast = false
  }

  private def registerBroadcast(uuid: UUID, gInfo: SourceInfo): Unit = {
    val socket = new Socket(Broadcast.MasterHostAddress,
      Broadcast.MasterTrackerPort)
    val oosST = new ObjectOutputStream(socket.getOutputStream)
    oosST.flush()
    val oisST = new ObjectInputStream(socket.getInputStream)

    // Send messageType/intention
    oosST.writeObject(Broadcast.REGISTER_BROADCAST_TRACKER)
    oosST.flush()

    // Send UUID of this broadcast
    oosST.writeObject(uuid)
    oosST.flush()

    // Send this tracker's information
    oosST.writeObject(gInfo)
    oosST.flush()

    // Receive ACK and throw it away
    oisST.readObject.asInstanceOf[Int]

    // Shut stuff down
    oisST.close()
    oosST.close()
    socket.close()
  }

  private def unregisterBroadcast(uuid: UUID): Unit = {
    val socket = new Socket(Broadcast.MasterHostAddress,
      Broadcast.MasterTrackerPort)
    val oosST = new ObjectOutputStream(socket.getOutputStream)
    oosST.flush()
    val oisST = new ObjectInputStream(socket.getInputStream)

    // Send messageType/intention
    oosST.writeObject(Broadcast.UNREGISTER_BROADCAST_TRACKER)
    oosST.flush()

    // Send UUID of this broadcast
    oosST.writeObject(uuid)
    oosST.flush()

    // Receive ACK and throw it away
    oisST.readObject.asInstanceOf[Int]

    // Shut stuff down
    oisST.close()
    oosST.close()
    socket.close()
  }

  private def getLocalSourceInfo: SourceInfo = {
    // Wait till hostName and listenPort are OK
    while (listenPort == -1) {
      listenPortLock.synchronized {
        listenPortLock.wait
      }
    }

    // Wait till totalBlocks and totalBytes are OK
    while (totalBlocks == -1) {
      totalBlocksLock.synchronized {
        totalBlocksLock.wait
      }
    }

    var localSourceInfo = SourceInfo(
      hostAddress, listenPort, totalBlocks, totalBytes, blockSize)

    localSourceInfo.hasBlocks = hasBlocks.get

    hasBlocksBitVector.synchronized {
      localSourceInfo.hasBlocksBitVector = hasBlocksBitVector
    }

    return localSourceInfo
  }

  // Add new SourceInfo to the listOfSources. Update if it exists already.
  // TODO: Optimizing just by OR-ing the BitVectors was BAD for performance
  private def addToListOfSources(newSourceInfo: SourceInfo): Unit = {
    listOfSources.synchronized {
      if (listOfSources.contains(newSourceInfo)) {
        listOfSources = listOfSources - newSourceInfo
      }
      listOfSources += newSourceInfo
    }
  }

  private def addToListOfSources(newSourceInfos: ListBuffer[SourceInfo]): Unit = {
    newSourceInfos.foreach { newSourceInfo =>
      addToListOfSources(newSourceInfo)
    }
  }

  class TalkToGuide(gInfo: SourceInfo)
  extends Thread with Logging {
    override def run: Unit = {

      // Keep exchaning information until all blocks have been received
      while (hasBlocks.get < totalBlocks) {
        talkOnce
        Thread.sleep(BitTorrentBroadcast.ranGen.nextInt(
          Broadcast.MaxKnockInterval - Broadcast.MinKnockInterval) +
          Broadcast.MinKnockInterval)
      }

      // Talk one more time to let the Guide know of reception completion
      talkOnce
    }

    // Connect to Guide and send this worker's information
    private def talkOnce: Unit = {
      var clientSocketToGuide: Socket = null
      var oosGuide: ObjectOutputStream = null
      var oisGuide: ObjectInputStream = null

      clientSocketToGuide = new Socket(gInfo.hostAddress, gInfo.listenPort)
      oosGuide = new ObjectOutputStream(clientSocketToGuide.getOutputStream)
      oosGuide.flush()
      oisGuide = new ObjectInputStream(clientSocketToGuide.getInputStream)

      // Send local information
      oosGuide.writeObject(getLocalSourceInfo)
      oosGuide.flush()

      // Receive source information from Guide
      var suitableSources =
        oisGuide.readObject.asInstanceOf[ListBuffer[SourceInfo]]
      logInfo("Received suitableSources from Master " + suitableSources)

      addToListOfSources(suitableSources)

      oisGuide.close()
      oosGuide.close()
      clientSocketToGuide.close()
    }
  }

  def getGuideInfo(variableUUID: UUID): SourceInfo = {
    var clientSocketToTracker: Socket = null
    var oosTracker: ObjectOutputStream = null
    var oisTracker: ObjectInputStream = null

    var gInfo: SourceInfo = SourceInfo("", SourceInfo.TxOverGoToHDFS)

    var retriesLeft = Broadcast.MaxRetryCount
    do {
      try {
        // Connect to the tracker to find out GuideInfo
        clientSocketToTracker =
          new Socket(Broadcast.MasterHostAddress, Broadcast.MasterTrackerPort)
        oosTracker =
          new ObjectOutputStream(clientSocketToTracker.getOutputStream)
        oosTracker.flush()
        oisTracker =
          new ObjectInputStream(clientSocketToTracker.getInputStream)

        // Send messageType/intention
        oosTracker.writeObject(Broadcast.FIND_BROADCAST_TRACKER)
        oosTracker.flush()

        // Send UUID and receive GuideInfo
        oosTracker.writeObject(uuid)
        oosTracker.flush()
        gInfo = oisTracker.readObject.asInstanceOf[SourceInfo]
      } catch {
        case e: Exception => {
          logInfo("getGuideInfo had a " + e)
        }
      } finally {
        if (oisTracker != null) {
          oisTracker.close()
        }
        if (oosTracker != null) {
          oosTracker.close()
        }
        if (clientSocketToTracker != null) {
          clientSocketToTracker.close()
        }
      }

      Thread.sleep(BitTorrentBroadcast.ranGen.nextInt(
        Broadcast.MaxKnockInterval - Broadcast.MinKnockInterval) +
        Broadcast.MinKnockInterval)

      retriesLeft -= 1
    } while (retriesLeft > 0 && gInfo.listenPort == SourceInfo.TxNotStartedRetry)

    logInfo("Got this guidePort from Tracker: " + gInfo.listenPort)
    return gInfo
  }

  def receiveBroadcast(variableUUID: UUID): Boolean = {
    val gInfo = getGuideInfo(variableUUID)

    if (gInfo.listenPort == SourceInfo.TxOverGoToHDFS ||
        gInfo.listenPort == SourceInfo.TxNotStartedRetry) {
      // TODO: SourceInfo.TxNotStartedRetry is not really in use because we go
      // to HDFS anyway when receiveBroadcast returns false
      return false
    }

    // Wait until hostAddress and listenPort are created by the
    // ServeMultipleRequests thread
    while (listenPort == -1) {
      listenPortLock.synchronized {
        listenPortLock.wait
      }
    }

    // Setup initial states of variables
    totalBlocks = gInfo.totalBlocks
    arrayOfBlocks = new Array[BroadcastBlock](totalBlocks)
    hasBlocksBitVector = new BitSet(totalBlocks)
    numCopiesSent = new Array[Int](totalBlocks)
    totalBlocksLock.synchronized {
      totalBlocksLock.notifyAll
    }
    totalBytes = gInfo.totalBytes
    blockSize = gInfo.blockSize

    // Start ttGuide to periodically talk to the Guide
    var ttGuide = new TalkToGuide(gInfo)
    ttGuide.setDaemon(true)
    ttGuide.start()
    logInfo("TalkToGuide started...")

    // Start pController to run TalkToPeer threads
    var pcController = new PeerChatterController
    pcController.setDaemon(true)
    pcController.start()
    logInfo("PeerChatterController started...")

    // FIXME: Must fix this. This might never break if broadcast fails.
    // We should be able to break and send false. Also need to kill threads
    while (hasBlocks.get < totalBlocks) {
      Thread.sleep(Broadcast.MaxKnockInterval)
    }

    return true
  }

  class PeerChatterController
  extends Thread with Logging {
    private var peersNowTalking = ListBuffer[SourceInfo]()
    // TODO: There is a possible bug with blocksInRequestBitVector when a
    // certain bit is NOT unset upon failure resulting in an infinite loop.
    private var blocksInRequestBitVector = new BitSet(totalBlocks)

    override def run: Unit = {
      var threadPool = Utils.newDaemonFixedThreadPool(Broadcast.MaxRxSlots)

      while (hasBlocks.get < totalBlocks) {
        var numThreadsToCreate =
          math.min(listOfSources.size, Broadcast.MaxRxSlots) -
          threadPool.getActiveCount

        while (hasBlocks.get < totalBlocks && numThreadsToCreate > 0) {
          var peerToTalkTo = pickPeerToTalkToRandom

          if (peerToTalkTo != null)
            logInfo("Peer chosen: " + peerToTalkTo + " with " + peerToTalkTo.hasBlocksBitVector)
          else
            logInfo("No peer chosen...")

          if (peerToTalkTo != null) {
            threadPool.execute(new TalkToPeer(peerToTalkTo))

            // Add to peersNowTalking. Remove in the thread. We have to do this
            // ASAP, otherwise pickPeerToTalkTo picks the same peer more than once
            peersNowTalking.synchronized {
              peersNowTalking += peerToTalkTo
            }
          }

          numThreadsToCreate = numThreadsToCreate - 1
        }

        // Sleep for a while before starting some more threads
        Thread.sleep(Broadcast.MinKnockInterval)
      }
      // Shutdown the thread pool
      threadPool.shutdown()
    }

    // Right now picking the one that has the most blocks this peer wants
    // Also picking peer randomly if no one has anything interesting
    private def pickPeerToTalkToRandom: SourceInfo = {
      var curPeer: SourceInfo = null
      var curMax = 0

      logInfo("Picking peers to talk to...")

      // Find peers that are not connected right now
      var peersNotInUse = ListBuffer[SourceInfo]()
      listOfSources.synchronized {
        peersNowTalking.synchronized {
          peersNotInUse = listOfSources -- peersNowTalking
        }
      }

      // Select the peer that has the most blocks that this receiver does not
      peersNotInUse.foreach { eachSource =>
        var tempHasBlocksBitVector: BitSet = null
        hasBlocksBitVector.synchronized {
          tempHasBlocksBitVector = hasBlocksBitVector.clone.asInstanceOf[BitSet]
        }
        tempHasBlocksBitVector.flip(0, tempHasBlocksBitVector.size)
        tempHasBlocksBitVector.and(eachSource.hasBlocksBitVector)

        if (tempHasBlocksBitVector.cardinality > curMax) {
          curPeer = eachSource
          curMax = tempHasBlocksBitVector.cardinality
        }
      }

      // TODO: Always pick randomly or randomly pick randomly?
      // Now always picking randomly
      if (curPeer == null && peersNotInUse.size > 0) {
        // Pick uniformly the i'th required peer
        var i = BitTorrentBroadcast.ranGen.nextInt(peersNotInUse.size)

        var peerIter = peersNotInUse.iterator
        curPeer = peerIter.next

        while (i > 0) {
          curPeer = peerIter.next
          i = i - 1
        }
      }

      return curPeer
    }

    // Picking peer with the weight of rare blocks it has
    private def pickPeerToTalkToRarestFirst: SourceInfo = {
      // Find peers that are not connected right now
      var peersNotInUse = ListBuffer[SourceInfo]()
      listOfSources.synchronized {
        peersNowTalking.synchronized {
          peersNotInUse = listOfSources -- peersNowTalking
        }
      }

      // Count the number of copies of each block in the neighborhood
      var numCopiesPerBlock = Array.tabulate [Int](totalBlocks)(_ => 0)

      listOfSources.synchronized {
        listOfSources.foreach { eachSource =>
          for (i <- 0 until totalBlocks) {
            numCopiesPerBlock(i) +=
              ( if (eachSource.hasBlocksBitVector.get(i)) 1 else 0 )
          }
        }
      }

      // TODO: A block is rare if there are at most 2 copies of that block
      // TODO: This CONSTANT could be a function of the neighborhood size
      var rareBlocksIndices = ListBuffer[Int]()
      for (i <- 0 until totalBlocks) {
        if (numCopiesPerBlock(i) > 0 && numCopiesPerBlock(i) <= 2) {
          rareBlocksIndices += i
        }
      }

      // Find peers with rare blocks
      var peersWithRareBlocks = ListBuffer[(SourceInfo, Int)]()
      var totalRareBlocks = 0

      peersNotInUse.foreach { eachPeer =>
        var hasRareBlocks = 0
        rareBlocksIndices.foreach { rareBlock =>
          if (eachPeer.hasBlocksBitVector.get(rareBlock)) {
            hasRareBlocks += 1
          }
        }

        if (hasRareBlocks > 0) {
          peersWithRareBlocks += ((eachPeer, hasRareBlocks))
        }
        totalRareBlocks += hasRareBlocks
      }

      // Select a peer from peersWithRareBlocks based on weight calculated from
      // unique rare blocks
      var selectedPeerToTalkTo: SourceInfo = null

      if (peersWithRareBlocks.size > 0) {
        // Sort the peers based on how many rare blocks they have
        peersWithRareBlocks.sortBy(_._2)

        var randomNumber = BitTorrentBroadcast.ranGen.nextDouble
        var tempSum = 0.0

        var i = 0
        do {
          tempSum += (1.0 * peersWithRareBlocks(i)._2 / totalRareBlocks)
          if (tempSum >= randomNumber) {
            selectedPeerToTalkTo = peersWithRareBlocks(i)._1
          }
          i += 1
        } while (i < peersWithRareBlocks.size && selectedPeerToTalkTo == null)
      }

      if (selectedPeerToTalkTo == null) {
        selectedPeerToTalkTo = pickPeerToTalkToRandom
      }

      return selectedPeerToTalkTo
    }

    class TalkToPeer(peerToTalkTo: SourceInfo)
    extends Thread with Logging {
      private var peerSocketToSource: Socket = null
      private var oosSource: ObjectOutputStream = null
      private var oisSource: ObjectInputStream = null

      override def run: Unit = {
        // TODO: There is a possible bug here regarding blocksInRequestBitVector
        var blockToAskFor = -1

        // Setup the timeout mechanism
        var timeOutTask = new TimerTask {
          override def run: Unit = {
            cleanUpConnections()
          }
        }

        var timeOutTimer = new Timer
        timeOutTimer.schedule(timeOutTask, Broadcast.MaxKnockInterval)

        logInfo("TalkToPeer started... => " + peerToTalkTo)

        try {
          // Connect to the source
          peerSocketToSource =
            new Socket(peerToTalkTo.hostAddress, peerToTalkTo.listenPort)
          oosSource =
            new ObjectOutputStream(peerSocketToSource.getOutputStream)
          oosSource.flush()
          oisSource =
            new ObjectInputStream(peerSocketToSource.getInputStream)

          // Receive latest SourceInfo from peerToTalkTo
          var newPeerToTalkTo = oisSource.readObject.asInstanceOf[SourceInfo]
          // Update listOfSources
          addToListOfSources(newPeerToTalkTo)

          // Turn the timer OFF, if the sender responds before timeout
          timeOutTimer.cancel

          // Send the latest SourceInfo
          oosSource.writeObject(getLocalSourceInfo)
          oosSource.flush()

          var keepReceiving = true

          while (hasBlocks.get < totalBlocks && keepReceiving) {
            blockToAskFor =
              pickBlockRandom(newPeerToTalkTo.hasBlocksBitVector)

            // No block to request
            if (blockToAskFor < 0) {
              // Nothing to receive from newPeerToTalkTo
              keepReceiving = false
            } else {
              // Let other threads know that blockToAskFor is being requested
              blocksInRequestBitVector.synchronized {
                blocksInRequestBitVector.set(blockToAskFor)
              }

              // Start with sending the blockID
              oosSource.writeObject(blockToAskFor)
              oosSource.flush()

              // CHANGED: Master might send some other block than the one
              // requested to ensure fast spreading of all blocks.
              val recvStartTime = System.currentTimeMillis
              val bcBlock = oisSource.readObject.asInstanceOf[BroadcastBlock]
              val receptionTime = (System.currentTimeMillis - recvStartTime)

              logInfo("Received block: " + bcBlock.blockID + " from " + peerToTalkTo + " in " + receptionTime + " millis.")

              if (!hasBlocksBitVector.get(bcBlock.blockID)) {
                arrayOfBlocks(bcBlock.blockID) = bcBlock

                // Update the hasBlocksBitVector first
                hasBlocksBitVector.synchronized {
                  hasBlocksBitVector.set(bcBlock.blockID)
                  hasBlocks.getAndIncrement
                }

                rxSpeeds.addDataPoint(peerToTalkTo, receptionTime)

                // Some block(may NOT be blockToAskFor) has arrived.
                // In any case, blockToAskFor is not in request any more
                blocksInRequestBitVector.synchronized {
                  blocksInRequestBitVector.set(blockToAskFor, false)
                }

                // Reset blockToAskFor to -1. Else it will be considered missing
                blockToAskFor = -1
              }

              // Send the latest SourceInfo
              oosSource.writeObject(getLocalSourceInfo)
              oosSource.flush()
            }
          }
        } catch {
          // EOFException is expected to happen because sender can break
          // connection due to timeout
          case eofe: java.io.EOFException => { }
          case e: Exception => {
            logInfo("TalktoPeer had a " + e)
            // FIXME: Remove 'newPeerToTalkTo' from listOfSources
            // We probably should have the following in some form, but not
            // really here. This exception can happen if the sender just breaks connection
            // listOfSources.synchronized {
              // logInfo("Exception in TalkToPeer. Removing source: " + peerToTalkTo)
              // listOfSources = listOfSources - peerToTalkTo
            // }
          }
        } finally {
          // blockToAskFor != -1 => there was an exception
          if (blockToAskFor != -1) {
            blocksInRequestBitVector.synchronized {
              blocksInRequestBitVector.set(blockToAskFor, false)
            }
          }

          cleanUpConnections()
        }
      }

      // Right now it picks a block uniformly that this peer does not have
      private def pickBlockRandom(txHasBlocksBitVector: BitSet): Int = {
        var needBlocksBitVector: BitSet = null

        // Blocks already present
        hasBlocksBitVector.synchronized {
          needBlocksBitVector = hasBlocksBitVector.clone.asInstanceOf[BitSet]
        }

        // Include blocks already in transmission ONLY IF
        // BitTorrentBroadcast.EndGameFraction has NOT been achieved
        if ((1.0 * hasBlocks.get / totalBlocks) < Broadcast.EndGameFraction) {
          blocksInRequestBitVector.synchronized {
            needBlocksBitVector.or(blocksInRequestBitVector)
          }
        }

        // Find blocks that are neither here nor in transit
        needBlocksBitVector.flip(0, needBlocksBitVector.size)

        // Blocks that should/can be requested
        needBlocksBitVector.and(txHasBlocksBitVector)

        if (needBlocksBitVector.cardinality == 0) {
          return -1
        } else {
          // Pick uniformly the i'th required block
          var i = BitTorrentBroadcast.ranGen.nextInt(needBlocksBitVector.cardinality)
          var pickedBlockIndex = needBlocksBitVector.nextSetBit(0)

          while (i > 0) {
            pickedBlockIndex =
              needBlocksBitVector.nextSetBit(pickedBlockIndex + 1)
            i -= 1
          }

          return pickedBlockIndex
        }
      }

      // Pick the block that seems to be the rarest across sources
      private def pickBlockRarestFirst(txHasBlocksBitVector: BitSet): Int = {
        var needBlocksBitVector: BitSet = null

        // Blocks already present
        hasBlocksBitVector.synchronized {
          needBlocksBitVector = hasBlocksBitVector.clone.asInstanceOf[BitSet]
        }

        // Include blocks already in transmission ONLY IF
        // BitTorrentBroadcast.EndGameFraction has NOT been achieved
        if ((1.0 * hasBlocks.get / totalBlocks) < Broadcast.EndGameFraction) {
          blocksInRequestBitVector.synchronized {
            needBlocksBitVector.or(blocksInRequestBitVector)
          }
        }

        // Find blocks that are neither here nor in transit
        needBlocksBitVector.flip(0, needBlocksBitVector.size)

        // Blocks that should/can be requested
        needBlocksBitVector.and(txHasBlocksBitVector)

        if (needBlocksBitVector.cardinality == 0) {
          return -1
        } else {
          // Count the number of copies for each block across all sources
          var numCopiesPerBlock = Array.tabulate [Int](totalBlocks)(_ => 0)

          listOfSources.synchronized {
            listOfSources.foreach { eachSource =>
              for (i <- 0 until totalBlocks) {
                numCopiesPerBlock(i) +=
                  ( if (eachSource.hasBlocksBitVector.get(i)) 1 else 0 )
              }
            }
          }

          // Find the minimum
          var minVal = Integer.MAX_VALUE
          for (i <- 0 until totalBlocks) {
            if (numCopiesPerBlock(i) > 0 && numCopiesPerBlock(i) < minVal) {
              minVal = numCopiesPerBlock(i)
            }
          }

          // Find the blocks with the least copies that this peer does not have
          var minBlocksIndices = ListBuffer[Int]()
          for (i <- 0 until totalBlocks) {
            if (needBlocksBitVector.get(i) && numCopiesPerBlock(i) == minVal) {
              minBlocksIndices += i
            }
          }

          // Now select a random index from minBlocksIndices
          if (minBlocksIndices.size == 0) {
            return -1
          } else {
            // Pick uniformly the i'th index
            var i = BitTorrentBroadcast.ranGen.nextInt(minBlocksIndices.size)
            return minBlocksIndices(i)
          }
        }
      }

      private def cleanUpConnections(): Unit = {
        if (oisSource != null) {
          oisSource.close()
        }
        if (oosSource != null) {
          oosSource.close()
        }
        if (peerSocketToSource != null) {
          peerSocketToSource.close()
        }

        // Delete from peersNowTalking
        peersNowTalking.synchronized {
          peersNowTalking = peersNowTalking - peerToTalkTo
        }
      }
    }
  }

  class GuideMultipleRequests
  extends Thread with Logging {
    // Keep track of sources that have completed reception
    private var setOfCompletedSources = Set[SourceInfo]()

    override def run: Unit = {
      var threadPool = Utils.newDaemonCachedThreadPool()
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket(0)
      guidePort = serverSocket.getLocalPort
      logInfo("GuideMultipleRequests => " + serverSocket + " " + guidePort)

      guidePortLock.synchronized {
        guidePortLock.notifyAll
      }

      try {
        // Don't stop until there is a copy in HDFS
        while (!stopBroadcast || !hasCopyInHDFS) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout(Broadcast.ServerSocketTimeout)
            clientSocket = serverSocket.accept()
          } catch {
            case e: Exception => {
              logInfo("GuideMultipleRequests Timeout.")

              // Stop broadcast if at least one worker has connected and
              // everyone connected so far are done. Comparing with
              // listOfSources.size - 1, because it includes the Guide itself
              if (listOfSources.size > 1 &&
                setOfCompletedSources.size == listOfSources.size - 1) {
                stopBroadcast = true
              }
            }
          }
          if (clientSocket != null) {
            logInfo("Guide: Accepted new client connection:" + clientSocket)
            try {
              threadPool.execute(new GuideSingleRequest(clientSocket))
            } catch {
              // In failure, close the socket here; else, thread will close it
              case ioe: IOException => {
                clientSocket.close()
              }
            }
          }
        }

        // Shutdown the thread pool
        threadPool.shutdown()

        logInfo("Sending stopBroadcast notifications...")
        sendStopBroadcastNotifications

        unregisterBroadcast(uuid)
      } finally {
        if (serverSocket != null) {
          logInfo("GuideMultipleRequests now stopping...")
          serverSocket.close()
        }
      }
    }

    private def sendStopBroadcastNotifications: Unit = {
      listOfSources.synchronized {
        listOfSources.foreach { sourceInfo =>

          var guideSocketToSource: Socket = null
          var gosSource: ObjectOutputStream = null
          var gisSource: ObjectInputStream = null

          try {
            // Connect to the source
            guideSocketToSource =
              new Socket(sourceInfo.hostAddress, sourceInfo.listenPort)
            gosSource =
              new ObjectOutputStream(guideSocketToSource.getOutputStream)
            gosSource.flush()
            gisSource =
              new ObjectInputStream(guideSocketToSource.getInputStream)

            // Throw away whatever comes in
            gisSource.readObject.asInstanceOf[SourceInfo]

            // Send stopBroadcast signal. listenPort = SourceInfo.StopBroadcast
            gosSource.writeObject(SourceInfo("", SourceInfo.StopBroadcast))
            gosSource.flush()
          } catch {
            case e: Exception => {
              logInfo("sendStopBroadcastNotifications had a " + e)
            }
          } finally {
            if (gisSource != null) {
              gisSource.close()
            }
            if (gosSource != null) {
              gosSource.close()
            }
            if (guideSocketToSource != null) {
              guideSocketToSource.close()
            }
          }
        }
      }
    }

    class GuideSingleRequest(val clientSocket: Socket)
    extends Thread with Logging {
      private val oos = new ObjectOutputStream(clientSocket.getOutputStream)
      oos.flush()
      private val ois = new ObjectInputStream(clientSocket.getInputStream)

      private var sourceInfo: SourceInfo = null
      private var selectedSources: ListBuffer[SourceInfo] = null

      override def run: Unit = {
        try {
          logInfo("new GuideSingleRequest is running")
          // Connecting worker is sending in its information
          sourceInfo = ois.readObject.asInstanceOf[SourceInfo]

          // Select a suitable source and send it back to the worker
          selectedSources = selectSuitableSources(sourceInfo)
          logInfo("Sending selectedSources:" + selectedSources)
          oos.writeObject(selectedSources)
          oos.flush()

          // Add this source to the listOfSources
          addToListOfSources(sourceInfo)
        } catch {
          case e: Exception => {
            // Assuming exception caused by receiver failure: remove
            if (listOfSources != null) {
              listOfSources.synchronized {
                listOfSources = listOfSources - sourceInfo
              }
            }
          }
        } finally {
          ois.close()
          oos.close()
          clientSocket.close()
        }
      }

      // Randomly select some sources to send back
      private def selectSuitableSources(skipSourceInfo: SourceInfo): ListBuffer[SourceInfo] = {
        var selectedSources = ListBuffer[SourceInfo]()

        // If skipSourceInfo.hasBlocksBitVector has all bits set to 'true'
        // then add skipSourceInfo to setOfCompletedSources. Return blank.
        if (skipSourceInfo.hasBlocks == totalBlocks) {
          setOfCompletedSources.synchronized {
            setOfCompletedSources += skipSourceInfo
          }
          return selectedSources
        }

        listOfSources.synchronized {
          if (listOfSources.size <= Broadcast.MaxPeersInGuideResponse) {
            selectedSources = listOfSources.clone
          } else {
            var picksLeft = Broadcast.MaxPeersInGuideResponse
            var alreadyPicked = new BitSet(listOfSources.size)

            while (picksLeft > 0) {
              var i = -1

              do {
                i = BitTorrentBroadcast.ranGen.nextInt(listOfSources.size)
              } while (alreadyPicked.get(i))

              var peerIter = listOfSources.iterator
              var curPeer = peerIter.next

              // Set the BitSet before i is decremented
              alreadyPicked.set(i)

              while (i > 0) {
                curPeer = peerIter.next
                i = i - 1
              }

              selectedSources += curPeer

              picksLeft = picksLeft - 1
            }
          }
        }

        // Remove the receiving source (if present)
        selectedSources = selectedSources - skipSourceInfo

        return selectedSources
      }
    }
  }

  class ServeMultipleRequests
  extends Thread with Logging {
    // Server at most Broadcast.MaxTxSlots peers
    var threadPool = Utils.newDaemonFixedThreadPool(Broadcast.MaxTxSlots)

    override def run: Unit = {
      var serverSocket = new ServerSocket(0)
      listenPort = serverSocket.getLocalPort

      logInfo("ServeMultipleRequests started with " + serverSocket)

      listenPortLock.synchronized {
        listenPortLock.notifyAll
      }

      try {
        while (!stopBroadcast) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout(Broadcast.ServerSocketTimeout)
            clientSocket = serverSocket.accept()
          } catch {
            case e: Exception => {
              logInfo("ServeMultipleRequests Timeout.")
            }
          }
          if (clientSocket != null) {
            logInfo("Serve: Accepted new client connection:" + clientSocket)
            try {
              threadPool.execute(new ServeSingleRequest(clientSocket))
            } catch {
              // In failure, close socket here; else, the thread will close it
              case ioe: IOException => {
                clientSocket.close()
              }
            }
          }
        }
      } finally {
        if (serverSocket != null) {
          logInfo("ServeMultipleRequests now stopping...")
          serverSocket.close()
        }
      }
      // Shutdown the thread pool
      threadPool.shutdown()
    }

    class ServeSingleRequest(val clientSocket: Socket)
    extends Thread with Logging {
      private val oos = new ObjectOutputStream(clientSocket.getOutputStream)
      oos.flush()
      private val ois = new ObjectInputStream(clientSocket.getInputStream)

      logInfo("new ServeSingleRequest is running")

      override def run: Unit  = {
        try {
          // Send latest local SourceInfo to the receiver
          // In the case of receiver timeout and connection close, this will
          // throw a java.net.SocketException: Broken pipe
          oos.writeObject(getLocalSourceInfo)
          oos.flush()

          // Receive latest SourceInfo from the receiver
          var rxSourceInfo = ois.readObject.asInstanceOf[SourceInfo]

          if (rxSourceInfo.listenPort == SourceInfo.StopBroadcast) {
            stopBroadcast = true
          } else {
            // Carry on
            addToListOfSources(rxSourceInfo)
          }

          val startTime = System.currentTimeMillis
          var curTime = startTime
          var keepSending = true
          var numBlocksToSend = Broadcast.MaxChatBlocks

          while (!stopBroadcast && keepSending && numBlocksToSend > 0) {
            // Receive which block to send
            var blockToSend = ois.readObject.asInstanceOf[Int]

            // If it is master AND at least one copy of each block has not been
            // sent out already, MODIFY blockToSend
            if (BitTorrentBroadcast.isMaster && sentBlocks.get < totalBlocks) {
              blockToSend = sentBlocks.getAndIncrement
            }

            // Send the block
            sendBlock(blockToSend)
            rxSourceInfo.hasBlocksBitVector.set(blockToSend)

            numBlocksToSend -= 1

            // Receive latest SourceInfo from the receiver
            rxSourceInfo = ois.readObject.asInstanceOf[SourceInfo]
            // logInfo("rxSourceInfo: " + rxSourceInfo + " with " + rxSourceInfo.hasBlocksBitVector)
            addToListOfSources(rxSourceInfo)

            curTime = System.currentTimeMillis
            // Revoke sending only if there is anyone waiting in the queue
            if (curTime - startTime >= Broadcast.MaxChatTime &&
                threadPool.getQueue.size > 0) {
              keepSending = false
            }
          }
        } catch {
          // If something went wrong, e.g., the worker at the other end died etc.
          // then close everything up
          // Exception can happen if the receiver stops receiving
          case e: Exception => {
            logInfo("ServeSingleRequest had a " + e)
          }
        } finally {
          logInfo("ServeSingleRequest is closing streams and sockets")
          ois.close()
          // TODO: The following line causes a "java.net.SocketException: Socket closed"
          oos.close()
          clientSocket.close()
        }
      }

      private def sendBlock(blockToSend: Int): Unit = {
        try {
          oos.writeObject(arrayOfBlocks(blockToSend))
          oos.flush()
        } catch {
          case e: Exception => {
            logInfo("sendBlock had a " + e)
          }
        }
        logInfo("Sent block: " + blockToSend + " to " + clientSocket)
      }
    }
  }
}

class BitTorrentBroadcastFactory
extends BroadcastFactory {
  def initialize(isMaster: Boolean) = {
    BitTorrentBroadcast.initialize(isMaster)
  }

  def newBroadcast[T](value_ : T, isLocal: Boolean) =
    new BitTorrentBroadcast[T](value_, isLocal)
}

private object BitTorrentBroadcast
extends Logging {
  val values = SparkEnv.get.cache.newKeySpace()

  var valueToGuideMap = Map[UUID, SourceInfo]()

  // Random number generator
  var ranGen = new Random

  private var initialized = false
  private var isMaster_ = false

  private var trackMV: TrackMultipleValues = null

  def initialize(isMaster__ : Boolean): Unit = {
    synchronized {
      if (!initialized) {

        isMaster_ = isMaster__

        if (isMaster) {
          trackMV = new TrackMultipleValues
          trackMV.setDaemon(true)
          trackMV.start()
          // TODO: Logging the following line makes the Spark framework ID not
          // getting logged, cause it calls logInfo before log4j is initialized
          logInfo("TrackMultipleValues started...")
        }

        // Initialize DfsBroadcast to be used for broadcast variable persistence
        // TODO: Think about persistence
        DfsBroadcast.initialize

        initialized = true
      }
    }
  }

  def isMaster = isMaster_

  class TrackMultipleValues
  extends Thread with Logging {
    override def run: Unit = {
      var threadPool = Utils.newDaemonCachedThreadPool()
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket(Broadcast.MasterTrackerPort)
      logInfo("TrackMultipleValues" + serverSocket)

      try {
        while (true) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout(Broadcast.TrackerSocketTimeout)
            clientSocket = serverSocket.accept()
          } catch {
            case e: Exception => {
              logInfo("TrackMultipleValues Timeout. Stopping listening...")
            }
          }

          if (clientSocket != null) {
            try {
              threadPool.execute(new Thread {
                override def run: Unit = {
                  val oos = new ObjectOutputStream(clientSocket.getOutputStream)
                  oos.flush()
                  val ois = new ObjectInputStream(clientSocket.getInputStream)

                  try {
                    // First, read message type
                    val messageType = ois.readObject.asInstanceOf[Int]

                    if (messageType == Broadcast.REGISTER_BROADCAST_TRACKER) {
                      // Receive UUID
                      val uuid = ois.readObject.asInstanceOf[UUID]
                      // Receive hostAddress and listenPort
                      val gInfo = ois.readObject.asInstanceOf[SourceInfo]

                      // Add to the map
                      valueToGuideMap.synchronized {
                        valueToGuideMap += (uuid -> gInfo)
                      }

                      logInfo ("New broadcast registered with TrackMultipleValues " + uuid + " " + valueToGuideMap)

                      // Send dummy ACK
                      oos.writeObject(-1)
                      oos.flush()
                    } else if (messageType == Broadcast.UNREGISTER_BROADCAST_TRACKER) {
                      // Receive UUID
                      val uuid = ois.readObject.asInstanceOf[UUID]

                      // Remove from the map
                      valueToGuideMap.synchronized {
                        valueToGuideMap(uuid) = SourceInfo("", SourceInfo.TxOverGoToHDFS)
                        logInfo("Value unregistered from the Tracker " + valueToGuideMap)
                      }

                      logInfo ("Broadcast unregistered from TrackMultipleValues " + uuid + " " + valueToGuideMap)

                      // Send dummy ACK
                      oos.writeObject(-1)
                      oos.flush()
                    } else if (messageType == Broadcast.FIND_BROADCAST_TRACKER) {
                      // Receive UUID
                      val uuid = ois.readObject.asInstanceOf[UUID]

                      var gInfo =
                        if (valueToGuideMap.contains(uuid)) valueToGuideMap(uuid)
                        else SourceInfo("", SourceInfo.TxNotStartedRetry)

                      logInfo("TrackMultipleValues: Got new request: " + clientSocket + " for " + uuid + " : " + gInfo.listenPort)

                      // Send reply back
                      oos.writeObject(gInfo)
                      oos.flush()
                    } else if (messageType == Broadcast.GET_UPDATED_SHARE) {
                      // TODO: Not implemented
                    } else {
                      throw new SparkException("Undefined messageType at TrackMultipleValues")
                    }
                  } catch {
                    case e: Exception => {
                      logInfo("TrackMultipleValues had a " + e)
                    }
                  } finally {
                    ois.close()
                    oos.close()
                    clientSocket.close()
                  }
                }
              })
            } catch {
              // In failure, close socket here; else, client thread will close
              case ioe: IOException => {
                clientSocket.close()
              }
            }
          }
        }
      } finally {
        serverSocket.close()
      }
      // Shutdown the thread pool
      threadPool.shutdown()
    }
  }
}
