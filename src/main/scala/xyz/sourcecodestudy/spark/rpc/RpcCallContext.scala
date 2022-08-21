package xyz.sourcecodestudy.spark.rpc

trait RpcCallContext {

  def reply(response: Any): Unit 

  def sendFailure(e: Throwable): Unit

  def senderAddress: RpcAddress
}