package xyz.sourcecodestudy.spark.rpc.netty

import xyz.sourcecodestudy.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher) extends RpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RpcEndpointVerifier.CheckExistence(name) => context.reply(dispatcher.verify(name))
  }
}

object RpcEndpointVerifier {
  val NAME = "endpoint-verifier"

  case class CheckExistence(name: String)
}