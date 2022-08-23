package xyz.sourcecodestudy.spark.rpc

case class RpcEndpointAddress(rpcAddress: RpcAddress, name: String) {
  require(name != null, "RpcEndpoint name must be provided")

  def this(host: String, port: Int, name: String) = {
    this(RpcAddress(host, port), name)
  }

  override def toString(): String = rpcAddress match {
    case null => s"spark://${name}@${rpcAddress.host}:${rpcAddress.port}"
    case _    => s"spark-client://${name}"
  }
}

object RpcEndpointAddress {

  def apply(host: String, port:Int, name: String): RpcEndpointAddress = {
    new RpcEndpointAddress(host, port, name)
  }
}