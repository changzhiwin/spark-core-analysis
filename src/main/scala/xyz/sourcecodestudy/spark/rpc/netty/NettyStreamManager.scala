package xyz.sourcecodestudy.spark.rpc.netty

import org.apache.spark.network.server.{StreamManager, ManagedBuffer}

class NettyStreamManager(/*rpcEnv: NettyRpcEnv*/) extends StreamManager {

  def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
    throw new UnsupportedOperationException()
  }
}