package xyz.sourcecodestudy.spark.rpc.netty

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentHashMap
import java.nio.ByteBuffer
import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.io.{DataInputStream, ByteArrayInputStream}

import scala.util.control.NonFatal

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.{SparkConf}
import xyz.sourcecodestudy.spark.serializer.{JavaSerializer, JavaSerializerInstance}
import xyz.sourcecodestudy.spark.rpc.{RpcEndpointRef, RpcEndpoint}
import xyz.sourcecodestudy.spark.rpc.{RpcEnv, AbortableRpcFuture, RpcAddress, RpcEndpointAddress, RpcEnvStoppedException}

// Just mock
class TransportClient

class NettyRpcEnv(
    val conf: SparkConf, 
    javaSerializerInstance: JavaSerializerInstance,
    host: String,
    numUsableCores: Int) extends RpcEnv(conf) with Logging {
  
  val role = "executor"

  private var server = null // TransportServer

  private val stopped = new AtomicBoolean(false)

  override lazy val address: RpcAddress = {
    server match {
      // 暂未实现服务端
      case null => RpcAddress(host, 9999) //server.getPort())
      case _    => null
    }
  }

  private val dispatcher: Dispatcher = new Dispatcher(this, numUsableCores)

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
  }

  // 暂不实现
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  private def removeOutbox(address: RpcAddress): Unit = {
    val outbox = outboxes.remove(addrss)
    if (outbox != null) {
      outbox.stop()
    }
  }

  private def send(message: RequestMessage): Unit = {
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      try {
        dispatcher.postOneWayMessage(message)
      } catch {
        case e: RpcEnvStoppedException => logger.debug(e.getMessage)
      }
    } else {
      // remote PRC endpoint
    }
  }

  def askAbortable[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    val remoteAddr = message.receiver.address
    val promise = Promise[Any]()

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        e match {
          case re: RpcEnvStoppedException =>
            logger.debug(s"askAbortable -> onFailure_stop, ${re}")
          case _ =>
            logger.warn(s"askAbortable -> onFailure_, ${re}")
        }
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply      => 
        if (!promise.trySuccess(rpcReply)) {
          logger.warn(s"Ignored message: ${rpcReply}")
        }
    }

    def onAbort(t: Throwable): Unit = {
      onFailure(t)
      //
    }

    try {
      if (remoteAddr == address) {

        val p = Promise[Any]()
        p.future.onComplete {
          case Success(response) => onSuccess(response)
          case Failure(e) => onFailure(e)
        }(ThreadUtils.sameThread)
        
        dispatcher.postLocalMessage(message, p)
      } else {
        // TODO outbox
      }

      // 暂未实现超时逻辑，TODO

    } catch {
      case NonFatal(e) =>
        onFailure(e)
    }

    new AbortableRpcFuture[T](
      promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread),
      onAbort
    )
  }

  def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
    askAbortable(message, timeout).future
  }

  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpint)
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def serializeStream(out: OutputStream): SerializationStream = {
    javaSerializerInstance.serializeStream(out)
  }

  private def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    // client ?
    javaSerializerInstance.deserialize[T](bytes)
  }
}

class NettyRpcEnvFactory extends RpcEnvFactory {
  def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    val javaSerializerInstance = new JavaSerializer().newInstance().asInstanceOf[JavaSerializerInstance]
    val nettyEnv = new NettyRpcEnv(sparkConf, javaSerializerInstance, "127.0.0.1", 1)
    if (false/*!config.clientMode*/) {
      // TODO
    }
    nettyEnv
  }
}

case class RpcFailure(e: Throwable)

private[spark] class NettyRpcEndpointRef(
    @transient private val conf: SparkConf,
    private val endpointAddress: RpcEndpointAddress,
    @transient private var nettyEnv: NettyRpcEnv) extends RpcEndpointRef(conf) {

  @transient var client: TransportClient = _

  // 避免null的使用
  override def address: Option[RpcAddress] = Option(endpointAddress.rpcAddress) 

  override def name: String = endpointAddress.name

  override def askAbortable[T: ClassTag](message: Any, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    nettyEnv.askAbortable(new RequestMessage(nettyEnv.address, this, message), timeout)
  }

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    askAbortable(message, timeout).future
  }

  override def send(message: Any): Unit = {
    require(message != null, "Message is null")
    nettyEnv.send(new RequestMessage(nettyEnv.address, this, message))
  }

  override def toString: String = s"NettyRpcEndpointRef(${endpointAddress})"

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => endpointAddress == other.endpointAddress
    case _ => false
  }

  final override def hasCode(): Int = endpointAddress match {
    case null => 0
    case _    => endpointAddress.hashCode()
  }
}

private[netty] class RequestMessage(
    val senderAddress: RpcAddress,
    val receiver: NettyRpcEndpointRef,
    val content: Any) {
  
  def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)

    try {
      writeRpcAddress(out, Option(senderAddress))
      writeRpcAddress(out, receiver.address)
      out.writeUTF(receiver.name)

      val s = nettyEnv.serializeStream(out)
      try { s.writeObject(content) } finally { s.close() }

    } finally {
      out.close()
    }

    ByteBuffer.wrap(bos.toByteArray)
  }

  private def writeRpcAddress(out: DataOutputStream, rpcAddress: Option[RpcAddress]): Unit = {
    rpcAddress match {
      case None => out.writeBoolean(false)
      case Some(addr)    => {
        out.writeBoolean(true)
        out.writeUTF(addr.host)
        out.writeInt(addr.port)
      }
    }
  }

  override def toString: String = s"RequestMessage(${senderAddress}, ${receiver}, ${content})"
}

private[netty] object RequestMessage {
  
  private def readRpcAddress(in: DataInputStream): RpcAddress = {
    val hasRpcAddress = in.readBoolean()
    if (hasRpcAddress) {
      RpcAddress(in.readUTF(), in.readInt())
    } else {
      null
    }
  }

  def apply(nettyEnv: NettyRpcEnv, client: TransportClient, bytes: ByteBuffer): RequestMessage = {
    val bis = new ByteArrayInputStream(bytes.array())
    val in = new DataInputStream(bis)

    try {
      val senderAddress = readRpcAddress(in)
      val endpointAddress = RpcEndpointAddress(readRpcAddress(in), in.readUTF())
      val ref = new NettyRpcEndpointRef(nettyEnv.conf, endpointAddress, nettyEnv)
      ref.client = client
      new RequestMessage(
        senderAddress,
        ref, 
        nettyEnv.deserialize(client, bytes)
      )
    } finally {
      in.close()
    }
  }
}