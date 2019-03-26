DataSource部分详解

Source分类

1. 基于文件——readTextFile(path)
2. 基于集合——fromCollection(Collection)

Transformations部分详解

Transformations分类

常用操作

    Map：输入一个元素，然后返回一个元素，中间可以做一些清洗转换等操作
    FlatMap：输入一个元素，可以返回零个，一个或者多个元素
    MapPartition：类似map，一次处理一个分区的数据【如果在进行map处理的时候需要获取第三方资源链接，建议使用MapPartition】
    Filter：过滤函数，对传入的数据进行判断，符合条件的数据会被留下
    Reduce：对数据进行聚合操作，结合当前元素和上一次reduce返回的值进行聚合操作，然后返回一个新的值
    Aggregate：sum、max、min等
    Distinct：返回一个数据集中去重之后的元素，data.distinct()
    Join：内连接
    OuterJoin：外链接

数据集操作

    Cross：获取两个数据集的笛卡尔积
    Union：返回两个数据集的总和，数据类型需要一致
    First-n：获取集合中的前N个元素
    Sort Partition：在本地对数据集的所有分区进行排序，通过sortPartition()的链接调用来完成对多个字段的排序

分区操作

    Rebalance：对数据集进行再平衡，重分区，消除数据倾斜
    Hash-Partition：根据指定key的哈希值对数据集进行分区
    	partitionByHash()
    Range-Partition：根据指定的key对数据集进行范围分区
    	partitionByRange()
    Custom Partitioning：自定义分区规则
    	自定义分区需要实现Partitioner接口
    	partitionCustom(partitioner, "someKey")
    	或者partitionCustom(partitioner, 0)

Sink部分详解

Sink分类

1. writeAsText()：将元素以字符串形式逐行写入，这些字符串通过调用每个元素的toString()方法来获取
2. writeAsCsv()：将元组以逗号分隔写入文件中，行及字段之间的分隔是可配置的。每个字段的值来自对象的toString()方法
3. print()：打印每个元素的toString()方法的值到标准输出或者标准错误输出流中


数据类型DataType

    Java Tuple 和 Scala case class
    Java POJOs：java实体类
    Primitive Types：默认支持java和scala基本数据类型
    General Class Types：默认支持大多数java和scala class
    Hadoop Writables：支持hadoop中实现了org.apache.hadoop.Writable的数据类型
    Special Types：例如scala中的Either Option 和Try

序列化serialize

Flink自带了针对诸如int，long，String等标准类型的序列化器，针对Flink无法实现序列化的数据类型，我们可以交给Avro和Kryo

    使用方法：ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    使用avro序列化：env.getConfig().enableForceAvro();
    使用kryo序列化：env.getConfig().enableForceKryo();
    使用自定义序列化：env.getConfig().addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass)
    https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/custom_serializers.html

广播变量Broadcast

Broadcast(广播变量)把元素广播给所有的分区，数据会被重复处理。允许程序员将一个只读的变量缓存在每台机器上，而不用在任务之间传递变量。广播变量可以进行共享，但是不可以进行修改。

    dataStream.broadcast()

广播变量允许编程人员在每台机器上保持1个只读的缓存变量，而不是传送变量的副本给tasks

广播变量创建后，它可以运行在集群中的任何function上，而不需要多次传递给集群节点。另外需要记住，不应该修改广播变量，这样才能确保每个节点获取到的值都是一致的

一句话解释，可以理解为是一个公共的共享变量，我们可以把一个dataset 数据集广播出去，然后不同的task在节点上都能够获取到，这个数据在每个节点上只会存在一份。如果不使用broadcast，则在每个节点中的每个task中都需要拷贝一份dataset数据集，比较浪费内存(也就是一个节点中可能会存在多份dataset数据)。

    1：初始化数据
    	DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3)
    2：广播数据
    	.withBroadcastSet(toBroadcast, "broadcastSetName");
    3：获取数据
    	Collection<Integer> broadcastSet = getRuntimeContext().
    	getBroadcastVariable("broadcastSetName");
    
    注意：广播出去的变量存在于每个节点的内存中，所以这个数据集不能太大。因为广播出去的数据，会常驻内存，除非程序执行结束

累加器Accumulators & Counters

Accumulator即累加器，可以在不同任务中对同一个变量进行累加操作。与Mapreduce counter的应用场景差不多，都能很好地观察task在运行期间的数据变化

可以在Flink job任务中的算子函数中操作累加器，但是只能在任务执行结束之后才能获得累加器的最终结果。

Counter是一个具体的累加器(Accumulator)实现

IntCounter, LongCounter 和 DoubleCounter

    1：创建累加器
    	private IntCounter numLines = new IntCounter(); 
    2：注册累加器
    	getRuntimeContext().addAccumulator("num-lines", this.numLines);
    3：使用累加器
    	this.numLines.add(1); 
    4：获取累加器的结果
    	myJobExecutionResult.getAccumulatorResult("num-lines")

分布式缓存Distributed Cache

Flink提供了一个分布式缓存，类似于hadoop，可以使用户在并行函数中很方便的读取本地文件

此缓存的工作机制如下：程序注册一个文件或者目录(本地或者远程文件系统，例如hdfs或者s3)，通过

ExecutionEnvironment注册缓存文件并为它起一个名称。当程序执行，Flink自动将文件或者目录复制到所有taskmanager节点的本地文件系统，用户可以通过这个指定的名称查找文件或者目录，然后从taskmanager节点的本地文件系统访问它

    91：注册一个文件
    	env.registerCachedFile("hdfs:///path/to/your/file", "hdfsFile")  
    2：访问数据
    	File myFile = getRuntimeContext().getDistributedCache().getFile("hdfsFile");


