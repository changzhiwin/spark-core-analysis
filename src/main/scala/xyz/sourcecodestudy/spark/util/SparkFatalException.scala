package xyz.sourcecodestudy.spark.util

final class SparkFatalException(val throwable: Throwable) extends Exception(throwable)