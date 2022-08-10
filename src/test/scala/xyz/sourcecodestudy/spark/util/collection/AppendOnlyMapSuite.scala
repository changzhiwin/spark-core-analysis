package xyz.sourcecodestudy.spark.util.collection

import org.scalatest.funsuite.AnyFunSuite

class AppendOnlyMapSuite extends AnyFunSuite {

  test("update key A - Z with index") {
    val map = new AppendOnlyMap[Int, Char]()
    var ret: Seq[(Int, Char)] = Nil

    for( (ch: Char, idx: Int) <- ('A' to 'Y').zipWithIndex) {
      ret = (idx, ch) +: ret
      map.update(idx, ch)
    }
    
    assert(map.iterator.toArray.sortBy(t => t._1).toSeq == ret.sortBy(t => t._1).toSeq)
  }
}