package xyz.sourcecodestudy.spark.rpc.demo

//import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.SparkConf
import xyz.sourcecodestudy.spark.rpc.{RpcEnv, RpcEndpointAddress, RpcAddress}
import xyz.sourcecodestudy.spark.rpc.netty.NettyRpcEndpointRef

object Server1 {

  def main(args: Array[String]): Unit = {

    val rpcEnv = RpcEnv.create("PingPongEnv", "127.0.0.1", 9991, new SparkConf(true), 1)

    val endpoint = new PingPongEndpoint(rpcEnv)

    // register endpoint, for process remote messages.
    rpcEnv.setupEndpoint("ping-pong-endpoint", endpoint)

    rpcEnv.awaitTermination()
  }
}