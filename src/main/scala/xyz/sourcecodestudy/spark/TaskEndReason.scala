package xyz.sourcecodestudy.spark

sealed trait TaskEndReason

case object Success extends TaskEndReason

case object Resubmited extends TaskEndReason

case class FetchFailed(bmAddress: String, shuffleId: Int, mapId: Int, reduceId: Int) extends TaskEndReason

case class ExceptionFailure(className: String, description: String, stackTrace: Array[StackTraceElement]) extends TaskEndReason

case object TaskResultLost extends TaskEndReason

case object TaskKilled extends TaskEndReason

case object ExecutorLostFailure extends TaskEndReason

case object UnknowReason extends TaskEndReason