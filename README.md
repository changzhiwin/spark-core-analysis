# 这个项目是干什么的
看了许利杰老师的[这本书](https://book.douban.com/subject/35140409/)，老师在理论层面讲的比较清楚了，读起来也容易理解，但看完但总是感觉还差点什么。
于是动手起了这个项目，定了个小目标：实现RDD的逻辑。在实操的过程中，也确实了解更多的代码细节，填补了一些盲点。

## 如何开始的
说实话，直接从最新的代码库开始，是有些困难的（代码量庞大）；而且core部分理念变化不大，如是找了较老的版本来开始，参考了如下两个
- [commit](https://github.com/apache/spark/tree/5b021ce0990ec675afc6939cc2c06f041c973d17)
- [tag v1.0.0](https://github.com/apache/spark/tree/v1.0.0/)

## 运行环境
- Java 1.8
- Scala 2.13.8
```
sbt run
sbt test
```

# 当前提供的输出

## 整体看一下
![rdd-running-logic](./doc/img/rdd-running-logic.png)

## 实现的程度
- 目前只支持单机版本
- 完整的RDD核心逻辑（DGA、Shuffle、Aggregator）
```
// MainApp.scala
def main(args: Array[String]) = {

  val sc = new SparkContext()
  logger.trace(s"Enter application, master = ${sc.master}")

  val rdd0 = sc.parallelize(Seq("a", "aa", "aaa", "aaaa", "aaa", "aaa", "aa", "aaaa", "aaaa", "aaaa"), 3)

  //.foreachPartition(p => println(s"part ${p.toSeq}"))
  rdd0.map(k => (k, 1.toLong)).count().foreach(p => println(s"count ${p._1} -> ${p._2}")) 

  val rdd1 = sc.parallelize(Seq("aa" -> 1, "bb" -> 2, "aa" -> 3, "bc" -> 4, "bc" -> 5, "cc" -> 6, "ac" -> 7, "ac" -> 8, "ab" -> 9), 3)

  rdd1.groupByKey(2).foreach(p => println(s"group ${p._1} -> ${p._2.toSeq}"))
}

// Output
[info] running xyz.sourcecodestudy.spark.MainApp 
count aaaa -> 4
count a -> 1
count aaa -> 3
count aa -> 2
group ab -> List(9)
group bc -> List(4, 5)
group bb -> List(2)
group aa -> List(1, 3)
group cc -> List(6)
group ac -> List(7, 8)
[success] Total time: 4 s, completed 2022-8-10 15:17:35
```

# TODOs（还有很多特性没有实现...）
- 支持集群
- 支持cache
- 支持checkpoint