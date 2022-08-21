package xyz.sourcecodestudy.spark.rpc

import xyz.sourcecodestudy.spark.SparkConf

case class RpcEvnConfig(
    conf: SparkConf,
    name: String,
    bindAddress: String,
    port: Int,
    numUsableCores: Int)

object RpcEnv {
  def create(
      name: String,
      host: String,
      port: Int,
      conf: SparkConf,
      numUsableCores: Int): RpcEnv = {
    val config = RpcEvnConfig(conf, name, host, port, numUsableCores)
    new NettyRpcEnvFactory().create(config)
  }
}

abstract class RpcEnv(conf: SparkConf) {

  private def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  def address: RpcAddress

  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  def stop(endpint: RpcEndpointRef): Unit

  // Shutdown RpcEnv
  def shutdown(): Unit

  def awaitTermination(): Unit

  def deserialize[T](deserAction: () => T): T

}

