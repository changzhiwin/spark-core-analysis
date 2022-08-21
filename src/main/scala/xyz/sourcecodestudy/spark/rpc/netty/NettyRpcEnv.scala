package xyz.sourcecodestudy.spark.rpc.netty

import javax.xml.ws.Dispatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentHashMap
import java.nio.ByteBuffer
import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.io.{DataInputStream, ByteArrayInputStream}

class NettyRpcEnv(
    val conf: SparkConf, 
    javaSerializerInstance: JavaSerializerInstance,
    host: String,
    numUsableCores: Int) extends RpcEnv {
  
  val role = ???

  private var server: TransportServer = _

  private val stopped = new AtomicBoolean(false)

  override lazy val address: RpcAddress = {
    server match {
      case null => RpcAddress(host, server.getPort())
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

  override def endpoint(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpint)
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def serializeStream(out: OutputStream): SerializationStream = {
    javaSerializerInstance.serializeStram(out)
  }
}

private[netty] class NettyRpcEndpointRef(
    @transient private val conf: SparkConf,
    private val endpointAddress: RpcEndpointAddress,
    @transient private var nettyEnv: NettyEnv) extends RpcEndpointRef(conf) {

  @transient var client: TransportClient = _

  override def address: Option[RpcAddress] = Option(endpointAddress.rpcAddress) 

  override def name: String = endpointAddress.name
}

private[netty] class RequestMessage(
    val senderAddress: RpcAddress,
    val receiver: NettyRpcEndpointRef,
    val content: Any) {
  
  def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream()

    try {
      writeRpcAddress(out, senderAddress)
      writeRpcAddress(out, receiver.address)
      out.writeUTF(receiver.name)

      val s = nettyEnv.serializeStream(out)
      try { s.writeObject(content) } finally { s.close() }

    } finally {
      out.close()
    }

    ByteBuffer.wrap(bos.toByteArray)
  }

  private def writeRpcAddress(out: DataOutputStream, rpcAddress: RpcAddress): Unit = {
    rpcAddress match {
      case null => out.writeBoolean(false)
      case _    => {
        out.writeBoolean(true)
        out.writeUTF(rpcAddress.host)
        out.writeUTF(rpcAddress.port)
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
      val endpintAddress = RpcEndpointAddress(readRpcAddress(in), in.readUTF())
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