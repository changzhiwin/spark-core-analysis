package xyz.sourcecodestudy.spark.rpc.netty

import scala.util.control.NonFatal

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.SparkException
import xyz.sourcecodestudy.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}

private sealed trait InboxMessage

private case class OneWayMessage(senderAddress: RpcAddress, content: Any) extends InboxMessage

private case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage

private case object OnStart extends InboxMessage

private case object OnStop extends InboxMessage

private case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

private case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

private case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress) extends InboxMessage

/**
  * 收消息
  * @param endpointName
  * @param endpoint
  */
private class Inbox(val endpointName: String, val endpoint: RpcEndpoint) extends Logging {
  inbox => // 

  @GrardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  @GrardedBy("this")
  private var stopped = false

  @GrardedBy("this")
  private var enableConcurrent = false

  @GrardedBy("this")
  private var numActiveThreads = 0

  // 初始化一个启动的消息
  inbox.synchronized {
    messages.add(OnStart)
  }

  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null

    inbox.synchronized {

      if (!enableConcurrent && numActiveThreads != 0) return

      message = message.poll()
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }

    while (true) {
      safelyCall(endpoint) {
        message match {
          case RpcMessage(_sender, content, context) =>
            try {
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message ${message} from ${_sender}")
              })
            } catch {
              case e: Throwable =>
                // context.sendFailure(e)
                throw e
            }

          case OneWayMessage(_sender, content) =>
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message ${message} from ${_sender}")
            })

          case OnStart =>
            endpoint.onStart()
            // 是线程安全的，这个endpoint对应的Inbox允许多线程同时处理
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) enableConcurrent = true
              }
            }

          case OnStop =>
            val activeThreads = getNumActiveThreads
            assert(activeThreads == 1, s"There should be only a single active thread but foud ${activeThreads}.")
            dispatcher.removeRpcEndpointRef(endpoint)
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
          case RemoteProcessDisconnected(remoteAddress) =>
          case RemoteProcessConnectionError(cause, remoteAddress) =>
        }
      } // end safelyCall

      inbox.synchronized {
        if (!enableConcurrent && numActiveThreads != 1) {
          numActiveThreads -= 1
          return
        }
        message = messages.poll()
        // 没有取到message，会退出
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    } // end while(true)
  }

  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      onDrop(message)
    } else {
      messages.add(message)
    }
  }

  protected def onDrop(message: InboxMessage): Unit = {
    logger.warn(s"Drop ${message} because endpoint ${endpointName} is stopped")
  }

  def stop(): Unit = inbox.synchronized {
    if (!stopped) {
      enableConcurrent = false
      stopped = true
      messages.add(OnStop)
    }
  }

  def isEmpty: Boolean = index.synchronized { message.isEmpty }

  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {

    def dealWithFatalError(fatal: Throwable): Unit = {
      inbox.synchronized {
        assert(numActiveThreads > 0, "The number of active threads should be positive.")
        numActiveThreads -= 1
      }
      logger.error(s"An error happend while processing message in the inbox for ${endpoint}", fatal)
    }

    try action catch {
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) =>
            if (stopped) {
              logger.debug("Ignoring error", ee)
            } else {
              logger.error("Ignoring error", ee)
            }
          case fatal: Throwable =>
            dealWithFatalError(fatal)
        }
      case fatal: Throwable =>
        dealWithFatalError(fatal)
    }
  }

  def getNumActiveThreads: Int = {
    inbox.synchronized {
      numActiveThreads
    }
  }
}