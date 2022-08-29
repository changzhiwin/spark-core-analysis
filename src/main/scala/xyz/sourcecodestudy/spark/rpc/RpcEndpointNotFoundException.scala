package xyz.sourcecodestudy.spark.rpc

import xyz.sourcecodestudy.spark.SparkException

class RpcEndpointNotFoundException(uri: String) extends SparkException(s"Cannot find endpoint: $uri")