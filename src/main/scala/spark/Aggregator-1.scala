package spark

class Aggregator[K, V, C] (
    val createCombiner: V => C,
    val mergeValue: (C, V) => C,
    val mergeCombiners: (C, C) => C)
  extends Serializable
