package xyz.sourcecodestudy.spark

trait Partition extends Serializable {

  def index: Int

  override def hashCode(): Int = index

}