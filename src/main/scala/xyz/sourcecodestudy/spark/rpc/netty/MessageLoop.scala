package xyz.sourcecodestudy.spark.rpc.netty

import java.util.concurrent._

import scala.util.control.NonFatal

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.spark.SparkConf
import xyz.sourcecodestudy.spark.util.ThreadUtils
import xyz.sourcecodestudy.spark.rpc.{IsolatedRpcEndpoint, RpcEndpoint}

private sealed abstract class MessageLoop(dispatcher: Dispatcher) extends Logging {

  private val active = new LinkedBlockingDeque[Inbox]()

  protected val receiveLoopRunnable = new Runnable() {
    override def run(): Unit = receiveLoop()
  }

  protected val threadpool: ExecutorService

  private var stopped = false

  def post(endpointName: String, message: InboxMessage): Unit

  def unregister(name: String): Unit

  def stop(): Unit = {
    synchronized {
      if (!stopped) {
        setActive(MessageLoop.PoisonPill)
        threadpool.shutdown()
        stopped = true
      }
    } 
    // threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  protected final def setActive(inbox: Inbox): Unit = active.offer(inbox)

  private def receiveLoop(): Unit = {
    try {
      while (true) {
        try {
          val inbox = active.take()
          if (inbox == MessageLoop.PoisonPill) {
            // 再次放回，是为了其他线程也能看到毒药
            setActive(MessageLoop.PoisonPill)
            return
          }

          inbox.process(dispatcher)
        } catch {
          case NonFatal(e) => logger.error(e.getMessage, e)
        }
      }
    } catch {
      case _: InterruptedException =>
      case t: Throwable            =>
        try {
          // 非致命错误，继续
          threadpool.execute(receiveLoopRunnable)
        } finally {
          throw t
        }
    }
  }
}

private object MessageLoop {
  // 不再处理数据的标记
  val PoisonPill = new Inbox(null, null)
}

// MessageLoop子类实现，多个Inbox共享线程池，轮番调度
private class SharedMessageLoop(conf: SparkConf, dispatcher: Dispatcher, numUsableCores: Int) extends MessageLoop(dispatcher) {
  
  private val endpoints = new ConcurrentHashMap[String, Inbox]()

  private def getNumOfThreads(conf: SparkConf): Int = {
    // 忽略配置的读取
    if (numUsableCores > 0) numUsableCores
    else Runtime.getRuntime.availableProcessors()
  }

  override protected val threadpool: ExecutorService = {
    val numThreads = getNumOfThreads(null.asInstanceOf[SparkConf])
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    
    // 启动执行
    for (i <- 0 until numThreads) {
      pool.execute(receiveLoopRunnable)
    }
    pool
  }

  override def post(endpointName: String, message: InboxMessage): Unit = {
    val inbox = endpoints.get(endpointName)
    inbox.post(message)
    setActive(inbox)
  }

  override def unregister(name: String): Unit = {
    val inbox = endpoints.remove(name)
    if (inbox != null) {
      inbox.stop()
      // 再次激活，处理stop指令
      setActive(inbox)
    }
  }

  // 一旦注册到MessageLoop，就是这个Inbox处于活跃状态
  def register(name: String, endpoint: RpcEndpoint): Unit = {
    val inbox = new Inbox(name, endpoint)
    endpoints.put(name, inbox)
    setActive(inbox)
  }
}

// 有且仅有一个Inbox需要处理
private class DedicatedMessageLoop(name: String, endpoint: IsolatedRpcEndpoint, dispatcher: Dispatcher) extends MessageLoop(dispatcher) {

  private val inbox = new Inbox(name, endpoint)

  override protected val threadpool: ExecutorService = {
    // 不建议大于1
    if (endpoint.threadCount() > 1) {
      ThreadUtils.newDaemonCachedThreadPool(s"dispatcher-${name}", endpoint.threadCount())
    } else {
      ThreadUtils.newDaemonSingleThreadExecutor(s"dispatcher-${name}")
    }
  }

  // 这里启动了执行
  (1 to endpoint.threadCount()).foreach { _ =>
    threadpool.submit(receiveLoopRunnable)
  }

  setActive(inbox)

  override def post(endpointName: String, message: InboxMessage): Unit = {
    require(endpointName == name)
    inbox.post(message)
    setActive(inbox)
  }

  override def unregister(endpointName: String): Unit = {
    require(endpointName == name)
    inbox.stop()

    // 激活，处理最后的消息
    setActive(inbox)
    setActive(MessageLoop.PoisonPill)
    threadpool.shutdown()
  }
}