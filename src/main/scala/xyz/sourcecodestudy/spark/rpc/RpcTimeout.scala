package xyz.sourcecodestudy.spark.rpc

import scala.concurrent.{Awaitable, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NonFatal
import scala.concurrent.duration._

import java.util.concurrent.TimeoutException

import xyz.sourcecodestudy.spark.SparkException
import xyz.sourcecodestudy.spark.util.SparkFatalException

class RpcTimeoutException(message: String, cause: TimeoutException) extends TimeoutException(message) {
  initCause(cause)
}

class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String) extends Serializable {

  private def createRpcTimeoutException(te: TimeoutException): RpcTimeoutException = {
    new RpcTimeoutException(te.getMessage + ". This timeout is controlled by " + timeoutProp, te)
  }

  def addMessageIfTimeout[T]: PartialFunction[Throwable, T] = {
    case rte: RpcTimeoutException => throw rte
    case te: TimeoutException => throw createRpcTimeoutException(te)
    case e: _  => throw e
  }

  def awaitResult[T](future: Future[T]): T = {
    try {
      //TreadUtils.awaitResult(future, duration)
      // 没有用到，占个位置
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      future.result(duration)(awaitPermission)
    } catch {
      case e: SparkFatalException =>
        throw e.throwable
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new SparkException("Exception thrown in awaitResult: ", t)
    }.andThen(addMessageIfTimeout)
  }
}

object RpcTimeout {
  
  def apply(conf: SparkConf, timeoutProps: String, defaultValue: String): RpcTimeout = {
    // conf.getTimeAsSeconds(timeoutProp, defaultValue).seconds

    val timeout = defaultValue.takeWhile(c => c.isDigit).toLong.seconds
    new RpcTimeout(timeout, timeoutProps)
  }
}
