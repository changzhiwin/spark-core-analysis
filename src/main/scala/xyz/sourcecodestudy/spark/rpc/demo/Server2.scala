package xyz.sourcecodestudy.spark.rpc.demo

//import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.SparkConf
import xyz.sourcecodestudy.spark.rpc.{RpcEnv, RpcEndpointAddress, RpcAddress}
import xyz.sourcecodestudy.spark.rpc.netty.{NettyRpcEndpointRef, NettyRpcEnv}

object Server2 {

  def main(args: Array[String]): Unit = {

    val rpcEnv = RpcEnv.create("PingPongEnv", "127.0.0.1", 9992, new SparkConf(true), 1)

    // Just create a remote endpointRef, for send messages
    val endpointRef = new NettyRpcEndpointRef(
      rpcEnv.asInstanceOf[NettyRpcEnv].conf, 
      new RpcEndpointAddress("127.0.0.1", 9991, "ping-pong-endpoint"),
      rpcEnv.asInstanceOf[NettyRpcEnv])

    endpointRef.send(Notify("Hi, I am server2."))

    val ans = endpointRef.askSync[Pong](Ping(1))

    println(s"Get answer: $ans")

    rpcEnv.awaitTermination()
  }
}