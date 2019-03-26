DataSource部分详解

source是程序的数据源输入，你可以通过StreamExecutionEnvironment.addSource(sourceFunction)来为你的程序添加一个source。

flink提供了大量的已经实现好的source方法，你也可以自定义source

1. 通过实现sourceFunction接口来自定义无并行度的source，
2. 通过实现ParallelSourceFunction 接口 or 继承RichParallelSourceFunction 来自定义有并行度的source。

Sources分类

1. 基于文件——readTextFile(path)
读取文本文件，文件遵循TextInputFormat 读取规则，逐行读取并返回。
2. 基于socket——socketTextStream
从socker中读取数据，元素可以通过一个分隔符切开。
3. 基于集合——fromCollection(Collection)
通过java 的collection集合创建一个数据流，集合中的所有元素必须是相同类型的。
4. 自定义输入
addSource 可以实现读取第三方数据源的数据
系统内置提供了一批connectors，连接器会提供对应的source支持【kafka】

内置Connectors

- Apache Kafka (source/sink)
- Apache Cassandra (sink)
- Elasticsearch (sink)
- Hadoop FileSystem (sink)
- RabbitMQ (source/sink)
- Apache ActiveMQ (source/sink)
- Redis (sink)

Source容错性保证

  Source     	语义保证             	备注         
  kafka      	exactly once(仅一次)	建议使用0.10及以上
  Collections	exactly once     	           
  Files      	exactly once     	           
  Socktes    	at most once     	           

自定义source

1. 实现并行度为1的自定义source
   - 实现SourceFunction 
   - 一般不需要实现容错性保证
   - 处理好cancel方法(cancel应用的时候，这个方法会被调用)
2. 实现并行化的自定义source
   - 实现ParallelSourceFunction 
   - 或者继承RichParallelSourceFunction 

Transformations部分详解

Transformations分类

常用操作

    map：输入一个元素，然后返回一个元素，中间可以做一些清洗转换等操作
    flatmap：输入一个元素，可以返回零个，一个或者多个元素
    filter：过滤函数，对传入的数据进行判断，符合条件的数据会被留下
    keyBy：根据指定的key进行分组，相同key的数据会进入同一个分区【典型用法见备注】
    reduce：对数据进行聚合操作，结合当前元素和上一次reduce返回的值进行聚合操作，然后返回一个新的值
    aggregations：sum(),min(),max()等
    window：在后面单独详解

流合并与切分

    Union：合并多个流，新的流会包含所有流中的数据，但是union是一个限制，就是所有合并的流类型必须是一致的。
    Connect：和union类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法。
    CoMap, CoFlatMap：在ConnectedStreams中需要使用这种函数，类似于map和flatmap
    Split：根据规则把一个数据流切分为多个流
    Select：和split配合使用，选择切分后的流

分区操作

    Random partitioning：随机分区
    	dataStream.shuffle()
    Rebalancing：对数据集进行再平衡，重分区，消除数据倾斜
    	dataStream.rebalance()
    Rescaling：解释见备注
    	dataStream.rescale()
    Custom partitioning：自定义分区
    	自定义分区需要实现Partitioner接口
    	dataStream.partitionCustom(partitioner, "someKey")
    	或者dataStream.partitionCustom(partitioner, 0);
    Broadcasting：在后面单独详解

Sink部分详解

Sink分类

1. writeAsText()：将元素以字符串形式逐行写入，这些字符串通过调用每个元素的toString()方法来获取
2. print() / printToErr()：打印每个元素的toString()方法的值到标准输出或者标准错误输出流中
3. 自定义输出addSink【kafka、redis】

内置Connectors

- Apache Kafka (source/sink)
- Apache Cassandra (sink)
- Elasticsearch (sink)
- Hadoop FileSystem (sink)
- RabbitMQ (source/sink)
- Apache ActiveMQ (source/sink)
- Redis (sink)

Sink 容错性保证

  Sink         	语义保证                        	备注                                      
  hdfs         	exactly once                	                                        
  elasticsearch	at least once               	                                        
  kafka produce	at least once / exactly once	Kafka 0.9 and 0.10提供at least once<br>Kafka 0.11提供exactly once
  file         	at least once               	                                        
  redis        	at least once               	                                        

自定义sink

1. 实现自定义的sink
   - 实现SinkFunction接口
   - 或者继承RichSinkFunction
2. 参考org.apache.flink.streaming.connectors.redis.RedisSink
