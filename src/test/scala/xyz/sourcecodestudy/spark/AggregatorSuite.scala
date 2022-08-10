package xyz.sourcecodestudy.spark

// The FunSuite style: https://www.scalatest.org/user_guide/selecting_a_style
import org.scalatest.funsuite.AnyFunSuite

class AggregatorSuite extends AnyFunSuite{

  test("combine Seq[Int, Int](1 = 1, 2 = 2, 3 = 3)") {
    val createCombiner = (n: Int) => n
    val mergeValue = (s: Int, n: Int) => s + n
    val mergeCombiners = (s1: Int, s2: Int) => s1 + s2
    val agg = Aggregator[Int, Int, Int](createCombiner, mergeValue, mergeCombiners)

    val ret = agg.combineCombinersByKey(Seq((1 -> 1), (3 -> 1), (3 -> 1), (2 -> 1), (3 -> 1), (2 -> 1)).iterator, null).iterator.toSeq

    assert(ret.sortBy(k => k._1).toSeq == Seq((1 -> 1), (2 -> 2), (3 -> 3)))
  }

}