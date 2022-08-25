package xyz.sourcecodestudy.spark.rpc.netty

import java.nio.ByteBuffer
import java.net.{InetSocketAddress}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{RpcHandler, StreamManager}

import xyz.sourcecodestudy.spark.SparkException
import xyz.sourcecodestudy.spark.rpc.{RpcAddress}
import xyz.sourcecodestudy.spark.rpc.netty.{Dispatcher, NettyRpcEnv}


class NettyRpcHandler(
    dispatcher: Dispatcher,
    nettyEnv: NettyRpcEnv,
    streamManager: StreamManager) extends RpcHandler {

  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }
  
  override def receive(
      client: TransportClient,
      message: ByteBuffer): Unit = {
    
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  override def getStreamManager: StreamManager = streamManager

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    
    val address = Option(client.getSocketAddress().asInstanceOf[InetSocketAddress])
    address match {
        case Some(addr) => {
          val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
          dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))

          val remoteEnvAddress = Option(remoteAddresses.get(clientAddr))
          remoteEnvAddress match {
            case Some(raddr) => 
                dispatcher.postToAll(RemoteProcessConnectionError(cause, raddr))
            case None        =>  // do nothing
          }
        }
        case None       => {
            logger.error("Exception before connecting to the client", cause)
        }
    }
  }

  override def channelActive(client: TransportClient): Unit = {
     val address = Option(client.getSocketAddress().asInstanceOf[InetSocketAddress])
     address match {
        case Some(addr) => {
          val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
          dispatcher.postToAll(RemoteProcessConnected(clientAddr))
        }
        case None        => {
          throw new SparkException("No address after channel active.")
        }
     }
  }

  override def channelInactive(client: TransportClient): Unit = {
     val address = Option(client.getSocketAddress().asInstanceOf[InetSocketAddress])
     address match {
        case Some(addr) => {
          val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
          nettyEnv.removeOutbox(clientAddr)
          dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))

          val remoteEnvAddress = Option(remoteAddresses.get(clientAddr))
          remoteEnvAddress match {
            case Some(raddr) => 
                dispatcher.postToAll(RemoteProcessDisconnected(raddr))
            case None        =>  // do nothing
          }          
        }
        case None       => {
          // do nothing
        }
    }   
  }

  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMesage  = {
    val addr = client.getSocketAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)

    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    val requestMessage = RequestMessage(nettyEnv, client, message)
    assert(requestMessage.senderAddress != None)

    val remoteEnvAddress = requestMessage.senderAddress
    // clientAddr 表示是本次socket连接中远程的ip+port
    // remoteEnvAddress 表示是对方NettyEnv监听的ip+port
    if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
    }

    requestMessage
  }
}