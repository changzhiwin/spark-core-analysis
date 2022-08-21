package xyz.sourcecodestudy.spark.util

import xyz.sourcecodestudy.spark.SparkConf
import xyz.sourcecodestudy.spark.rpc.{RpcTimeout}

object RpcUtils {

  def askRpcTimeout(conf: SparkConf): RpcTimeout = {
    // RPC_ASK_TIMEOUT.key, NETWORK_TIMEOUT.key = Seq("spark.rpc.askTimeout", "spark.network.timeout")
    
    RpcTimeout(conf, "spark.rpc.defaultTimeout", "120s")
  }

}