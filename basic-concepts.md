#### RDD
分布式数据集合，逻辑概念，并不保存实际的数据；这种抽象使得，操作大规模数据集合，像本地的`Connection`一样方便。

#### Partition
并行计算的一个核心抽象；一个RDD底层分成多个Partition进行组织的。

#### Partitioner
Partition的算法抽象，常见的有Hash、Range；可以定制这个算法来实现和业务数据相关的Partition，来提升计算性能。

#### Dependency
计算是通过`transform`来表达的，在实现层面是由不同的`RDD`（MappedRDD/FilteredRDD等）来记录这些转换过程；
这些转换形成了一个链条，`Dependency`就是记录这个链条关系的，有如下两种：
- NarrowDependency，又细分为：OneToOneDependency、RangeDependency
- ShuffleDependency，这种需要重新组织数据，消耗大

#### Shuffle
当转换链遇到一个`ShuffleDependency`，就需要断链，因为这时候依赖的数据不能直接使用，需要重新组织。
例如`groupBy(key)`，某个`key`的数据分布在各个不同的`Partition`，需要将同`key`的数据规整到同一个`Partition`才能进行后续的处理。为了个好理解，我把`Shuffle`过程称为`N*M-M`：
```
// rdd1有N个partitions
// rdd2有M个partitions
val rdd2 = rdd1.groupBy(key)
```
- rdd1的每个partition，都需要输出M份结果，这个过程称为`shuffle write`
- rdd1有N个partition，所以一共产生N*M份结果
- rdd2有M个partition，计算的时候需要在rdd1的每个partition中找到属于自己的那一份，N份合一起才是一个partition，这个过程称为`shuffle read`

#### DAG
上文中提到的计算链，最终都可以泛化为有向无环图(DAG)。真正计算的时候，需要用这个图来作为运行的指南。因为是有向图，所以需要从 **最源头** 逐步往后推演。

#### Job
一个Job可以理解为一个DAG图，是一次真实的计算。

#### Stage
一个Job根据`ShuffleDependency`的数量拆成很多不同的Stage，同一个Stage中只有`NarrowDependency`。

#### Task
一个Stage根据所属RDD的partition个数，生成partition个Task并发执行。Task是独立的、最小的运行单元。