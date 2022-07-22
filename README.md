# 目的
深入理解Spark Core，目标是运行自己版本的RDD

# 进展
## 0722
1，从ShuffledRDD开始往下探索，追到了SimpleShuffleFetcher，查看了读取每个分片的细节；从文件流读取Object，但没有看到写入，猜想应该是DAG阶段写的
2，MapOutputTracker也是一个核心的数据结构，区分master/worker；里面主要管理shuffle的数据源信息

## 0721
1，clone了spark项目
2，找了一个很早很早的版本，作为理解的起点，commit-id = 5b021ce0990ec675afc6939cc2c06f041c973d17
3，理解SparkContext、SparkEnv，切入点是ParallelCollection
