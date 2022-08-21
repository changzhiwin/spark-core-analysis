package xyz.sourcecodestudy.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import xyz.sourcecodestudy.spark.SparkConf
import xyz.sourcecodestudy.spark.util.RpcUtils

private class RpcAbortException(message: String) extends Exception(message)

private class AbortableRpcFuture[T: ClassTag](val future: Future[T], onAbort: Throwable => Unit) {
  def abort(t: Throwable): Unit = onAbort(t)
}

private abstract class RpcEndpointRef(conf: SparkConf) extends Serializable {
  
  val defaultAskTimeout = RpcUtils.askRpcTimeout(null.asInstanceOf[SparkConf])

  def address: RpcAddress

  def name: String

  def send(message: Any): Unit

  def askAbortable[T: ClassTag](message: Any, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    throw new UnsupportedOperationException()
  }

  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  def askSync[T: ClassTag](message: Any): T = askAsync(message, defaultAskTimeout)

  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }
}