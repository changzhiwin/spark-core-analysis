package spark.examples

import spark.SparkContext
import spark.SparkContext._
import java.util.Random

object SimpleSkewedGroupByTest {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SimpleSkewedGroupByTest <host> " +
        "[numMappers] [numKVPairs] [valSize] [numReducers] [ratio]")
      System.exit(1)
    }  
    
    var numMappers = if (args.length > 1) args(1).toInt else 2
    var numKVPairs = if (args.length > 2) args(2).toInt else 1000
    var valSize = if (args.length > 3) args(3).toInt else 1000
    var numReducers = if (args.length > 4) args(4).toInt else numMappers
    var ratio = if (args.length > 5) args(5).toInt else 5.0

    val sc = new SparkContext(args(0), "GroupBy Test", System.getenv("SPARK_HOME"), List(System.getenv("SPARK_EXAMPLES_JAR")))
    
    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      var result = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        val offset = ranGen.nextInt(1000) * numReducers
        if (ranGen.nextDouble < ratio / (numReducers + ratio - 1)) {
          // give ratio times higher chance of generating key 0 (for reducer 0)
          result(i) = (offset, byteArr)
        } else {
          // generate a key for one of the other reducers
          val key = 1 + ranGen.nextInt(numReducers-1) + offset
          result(i) = (key, byteArr)
        }
      }
      result
    }.cache
    // Enforce that everything has been calculated and in cache
    pairs1.count
    
    println("RESULT: " + pairs1.groupByKey(numReducers).count)
    // Print how many keys each reducer got (for debugging)
    //println("RESULT: " + pairs1.groupByKey(numReducers)
    //                           .map{case (k,v) => (k, v.size)}
    //                           .collectAsMap)

    System.exit(0)
  }
}

