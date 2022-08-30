package xyz.sourcecodestudy.spark.rpc

import xyz.sourcecodestudy.spark.SparkException

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

  def apply(sparkUrl: String): RpcEndpointAddress = {
    try {
      val uri = new java.net.URI(sparkUrl)
      val host = uri.getHost()
      val port = uri.getPort()
      val name = uri.getUserInfo()
      if (uri.getScheme() != "spark" || host == null || port < 0 || name == null) {
        throw new SparkException(s"Invalid Spark URL: ${sparkUrl}")
      }
      new RpcEndpointAddress(host, port, name)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new SparkException(s"Invalid Spark URL: ${sparkUrl}", e)
    }
  }
}