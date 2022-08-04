# 目的
深入理解Spark Core，目标是运行自己版本的RDD

# 进展

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

// output
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