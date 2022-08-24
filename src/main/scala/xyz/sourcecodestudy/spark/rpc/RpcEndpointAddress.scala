package xyz.sourcecodestudy.spark.rpc

case class RpcEndpointAddress(rpcAddress: Option[RpcAddress], name: String) {
  require(name != null, "RpcEndpoint name must be provided")

  def this(host: String, port: Int, name: String) = {
    this(Some(RpcAddress(host, port)), name)
  }

  override def toString(): String = rpcAddress match {
    case Some(addr) => s"spark://${name}@${addr.host}:${addr.port}"
    case None    => s"spark-client://${name}"
  }
}

object RpcEndpointAddress {

  def apply(host: String, port:Int, name: String): RpcEndpointAddress = {
    new RpcEndpointAddress(host, port, name)
  }
}