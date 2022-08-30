package xyz.sourcecodestudy.spark.scheduler.cluster

import xyz.sourcecodestudy.spark.rpc.{RpcEndpointRef, RpcAddress}

class ExecutorInfo(val executorHost: String, val totalCores: Int) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[ExecutorInfo]

  override def equals(other: Any): Boolean = other match {
    case that: ExecutorInfo =>
      (that canEqual this) && executorHost == that.executorHost && totalCores == that.totalCores
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(executorHost, totalCores).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  
}

class ExecutorData(
    val executorEndpoint: RpcEndpointRef,
    val executorAddress: RpcAddress,
    override val executorHost: String,
    var freeCores: Int,
    override val totalCores: Int,
    val registrationTs: Long) extends ExecutorInfo(executorHost, totalCores)