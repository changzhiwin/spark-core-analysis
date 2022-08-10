# 开发环境
- Java 1.8.0_191
- Scala 2.13.8
```
// how to run
sbt run
sbt test
```

# 目的
深入理解Spark Core，目标是运行自己版本的RDD

# 进展

## 0810
1，支持groupByKey/count操作
```
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
2，增加unit test内容

## 0808
1，完成shuffle逻辑，支持repartition
```
def main(args: Array[String]) = { 

  val sc = new SparkContext()
  logger.trace(s"Enter application, master = ${sc.master}")

  val rdd = sc.parallelize(Seq("aa", "A", "bb", "B", "cc", "C", "dd", "D", "X"), 3)

  rdd.foreachPartition(iter => println(s"${Thread.currentThread().getName}, ${iter.toSeq}"))

  rdd.reHashPartition(n => n.size, 2).map(e => e._2).foreachPartition((iter => println(s"${Thread.currentThread().getName}, ${iter.toSeq}")))
}

// Output
[info] running xyz.sourcecodestudy.spark.MainApp 
Executor task lauch worker-0, List(aa, A, bb)
Executor task lauch worker-1, List(B, cc, C)
Executor task lauch worker-0, List(dd, D, X)
Executor task lauch worker-1, List(aa, bb, cc, dd)
Executor task lauch worker-2, List(A, B, C, D, X)
[success] Total time: 4 s, completed 2022-8-9 11:00:49
```

## 0804
1，支持collect，遇到了线程先后顺序导致的空指针问题，使用wait解决
```
object MainApp extends Logging {
  def main(args: Array[String]) = { 
    val sc = new SparkContext()
    logger.trace(s"Enter application, master = ${sc.master}")

    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    val result = rdd.map(n => n * 10).filter(n => n > 50).collect()
    logger.error(s"Result = ${result.toSeq}")

    val lens = sc.parallelize(Seq("spark", "foobar", "scala"), 3).map(n => n.length).collect()
    logger.error(s"lens = ${lens.toSeq}")
  }
}

// Output
2022-08-04 19:01:04 ERROR MainApp$: Result = ArraySeq(60, 70, 80, 90)
2022-08-04 19:01:05 ERROR MainApp$: lens = ArraySeq(5, 6, 5)
[success] Total time: 2 s, completed 2022-8-4 19:01:05
```

## 0803
1，解除所有errors
2，可运行NarrowDependency的map、filter，关键里程碑
```
object MainApp extends Logging {

  def main(args: Array[String]) = { 

    val sc = new SparkContext()
    logger.trace(s"Enter application, master = ${sc.master}")

    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 3).map(n => n * 10).filter(n => n > 30).foreach(println _)
  }
}
```

## 0725
1，完整理解MapOutputTracker、CacheTracker，前者用来管理shuffle的元数据，后者管理rdd缓存的元数据
2，了解LinkedHashMap用于LRU机制
3，了解maxMemory和-Xmx的关系

## 0723
1，进展较大，基本把整个逻辑都粗略过了一遍
2，弄得比较清楚地是shuffle write/read 如何衔接起来的逻辑，
3，弄得不怎么清楚的：多线程、master/worker、DAG/Stage、Cache、序列化具体实现


## 0722
1，从ShuffledRDD开始往下探索，追到了SimpleShuffleFetcher，查看了读取每个分片的细节；从文件流读取Object，但没有看到写入，猜想应该是DAG阶段写的
2，MapOutputTracker也是一个核心的数据结构，区分master/worker；里面主要管理shuffle的数据源信息

## 0721
1，clone了spark项目
2，找了一个很早很早的版本，作为理解的起点，commit-id = 5b021ce0990ec675afc6939cc2c06f041c973d17
3，理解SparkContext、SparkEnv，切入点是ParallelCollection

# 源码疑问

## TaskSetManager.statusUpdate 方法实现问题
`removeRunningTask` 被多余调用了，taskResultGetter里面会调用`taskSet.handleSuccessfulTask`，这个方法本身会自己调用`removeRunningTask`
```
            activeTaskSets.get(taskSetId).foreach { taskSet =>
              if (state == TaskState.FINISHED) {
                taskSet.removeRunningTask(tid)
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskSet.removeRunningTask(tid)
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
```