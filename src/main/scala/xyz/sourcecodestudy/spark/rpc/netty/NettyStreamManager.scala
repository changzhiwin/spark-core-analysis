package xyz.sourcecodestudy.spark.rpc.netty

import org.apache.spark.network.buffer.{ManagedBuffer}
import org.apache.spark.network.server.{StreamManager}

class NettyStreamManager(/*rpcEnv: NettyRpcEnv*/) extends StreamManager {

  def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
    throw new UnsupportedOperationException()
  }
}