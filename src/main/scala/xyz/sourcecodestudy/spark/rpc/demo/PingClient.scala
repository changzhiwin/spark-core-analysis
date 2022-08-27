package xyz.sourcecodestudy.spark.rpc.demo

import scala.util.{Success, Failure}

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.SparkConf
import xyz.sourcecodestudy.spark.rpc.{RpcEnv, RpcEndpointAddress}
import xyz.sourcecodestudy.spark.rpc.netty.{NettyRpcEndpointRef, NettyRpcEnv}
import xyz.sourcecodestudy.spark.util.ThreadUtils

object PingClient extends Logging {

  def main(args: Array[String]): Unit = {

    val rpcEnv = RpcEnv.create("PingPongEnv", "127.0.0.1", 9992, new SparkConf(true), 1)

    // Just create a remote endpointRef, for send messages
    val endpointRef = new NettyRpcEndpointRef(
      rpcEnv.asInstanceOf[NettyRpcEnv].conf, 
      new RpcEndpointAddress("127.0.0.1", 9991, "ping-pong-endpoint"),
      rpcEnv.asInstanceOf[NettyRpcEnv])

    // 1, send
    endpointRef.send(Notify("Hi, I am server2."))

    // 2, ask async
    endpointRef.ask[Pong](Ping(2)).onComplete {
      case Success(r) => logger.info(s"onComplete result = ${r}")
      case Failure(e) => logger.error(e)
    }(ThreadUtils.sameThread)

    // 3, ask sync
    val ans = endpointRef.askSync[Pong](Ping(1))
    logger.info(s"Get answer: $ans")

    //rpcEnv.awaitTermination()
  }
}