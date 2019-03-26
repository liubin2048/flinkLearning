状态(State)管理与恢复

我们前面写的word count的例子没有包含状态管理。如果一个task在处理过程中挂掉了，那么在内存中的状态都会丢失，所有的数据都需要重新计算。从容错和消息处理的语义上(at least once, exactly once)，Flink引入了state和checkpoint。首先区分一下两个概念：

- state一般指一个具体的task/operator的状态【state数据默认保存在java的堆内存中】。State可以被记录，在失败的情况下数据还可以恢复，Flink中有两种基本类型的State
Keyed State
Operator State
- checkpoint【可以理解为checkpoint是把state数据持久化存储了】，则表示了一个Flink Job在一个特定时刻的一份全局状态快照，即包含了所有task/operator的状态
- task是Flink中执行的基本单位。operator指transformation算子

    Keyed state和Operator State，可以以两种形式存在：
    1.托管状态（managed state）
    	由flink框架管理的状态
    2.原始状态（raw state)
    	由用户自行管理状态具体的数据结构，框架在做checkpoint的时候，使用byte[]来读写状态内容，对其内部数据结构一无所知。
    通常在DataStream上的状态推荐使用托管的状态，当实现也一个用户自定义的operator时，会使用到原始状态。

State-Keyed State

顾名思义，就是基于KeyedStream上的状态。这个状态是跟特定的key绑定的，对KeyedStream流上的每一个key，都对应一个state。

    stream.keyBy(…)

保存state的数据结构

- ValueState<T>:即类型为T的单值状态。这个状态与对应的key绑定，是最简单的状态了。它可以通过update方法更新状态值，通过value()方法获取状态值
- ListState<T>:即key上的状态值为一个列表。可以通过add方法往列表中附加值；也可以通过get()方法返回一个
- Iterable<T>来遍历状态值
- ReducingState<T>:这种状态通过用户传入的reduceFunction，每次调用add方法添加值的时候，会调用reduceFunction，最后合并到一个单一的状态值
- MapState<UK, UV>:即状态值为一个map。用户通过put或putAll方法添加元素

需要注意的是，以上所述的State对象，仅仅用于与状态进行交互（更新、删除、清空等），而真正的状态值，有可能是存在内存、磁盘、或者其他分布式存储系统中。相当于我们只是持有了这个状态的句柄

State-Operator State

与Key无关的State，与运算符Operator绑定的state，整个operator只对应一个state
保存state的数据结构	ListState<T>
举例来说，Flink中的Kafka Connector，就使用了operator state。它会在每个connector实例中，保存该实例中消费topic的所有(partition, offset)映射

状态容错

- 依靠checkPoint机制
- 保证exactly-once（只能保证Flink系统内的exactly-once，对于source和sink需要依赖外部的组件一同保证）

生成快照



恢复快照



checkPoint简介

为了保证state的容错性，Flink需要对state进行checkpoint。Checkpoint是Flink实现容错机制最核心的功能，它能够根据配置周期性地基于Stream中各个Operator/task的状态来生成快照，从而将这些状态数据定期持久化存储下来，当Flink程序一旦意外崩溃时，重新运行程序时可以有选择地从这些快照进行恢复，从而修正因为故障带来的程序数据异常

Flink的checkpoint机制可以与stream和state的持久化存储交互的前提：

- 持久化的source，它需要支持在一定时间内重放事件。这种sources的典型例子是持久化的消息队列（比如Apache Kafka，RabbitMQ等）或文件系统（比如HDFS，S3，GFS等）
- 用于state的持久化存储，例如分布式文件系统（比如HDFS，S3，GFS等）

checkPoint的配置

- 默认checkpoint功能是disabled的，想要使用的时候需要先启用
- checkpoint开启之后，默认的checkPointMode是Exactly-once
- checkpoint的checkPointMode有两种，Exactly-once和At-least-once
- Exactly-once对于大多数应用来说是最合适的。At-least-once可能用在某些延迟超低的应用程序（始终延迟为几毫秒）

    默认checkpoint功能是disabled的，想要使用的时候需要先启用
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(1000);
    
    // 高级选项：
    // 设置模式为exactly-once （这是默认值）
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    env.getCheckpointConfig().setCheckpointTimeout(60000);
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
    env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

State Backend(状态的后端存储)

默认情况下，state会保存在taskmanager的内存中，checkpoint会存储在JobManager的内存中。
state 的store和checkpoint的位置取决于State Backend的配置

    env.setStateBackend(…)

一共有三种State Backend

1. MemoryStateBackend
state数据保存在java堆内存中，执行checkpoint的时候，会把state的快照数据保存到jobmanager的内存中
基于内存的state backend在生产环境下不建议使用
2. FsStateBackend
state数据保存在taskmanager的内存中，执行checkpoint的时候，会把state的快照数据保存到配置的文件系统中
可以使用hdfs等分布式文件系统
3. RocksDBStateBackend
RocksDB跟上面的都略有不同，它会在本地文件系统中维护状态，state会直接写入本地rocksdb中。同时它需要配置一个远端的filesystem uri（一般是HDFS），在做checkpoint的时候，会把本地的数据直接复制到filesystem中。fail over的时候从filesystem中恢复到本地
   RocksDB克服了state受内存限制的缺点，同时又能够持久化到远端文件系统中，比较适合在生产中使用

    修改State Backend的两种方式
    
    第一种：单任务调整
        修改当前任务代码
        env.setStateBackend(new FsStateBackend("hdfs://namenode:9000/flink/checkpoints"));
        或者new MemoryStateBackend()
        或者new RocksDBStateBackend(filebackend, true);【需要添加第三方依赖】
    第二种：全局调整
        修改flink-conf.yaml
        state.backend: filesystem
        state.checkpoints.dir: hdfs://namenode:9000/flink/checkpoints
        注意：state.backend的值可以是下面几种：
        jobmanager(MemoryStateBackend), filesystem(FsStateBackend), rocksdb(RocksDBStateBackend)

Restart Strategies(重启策略)

Flink支持不同的重启策略，以在故障发生时控制作业如何重启
集群在启动时会伴随一个默认的重启策略，在没有定义具体重启策略时会使用该默认策略。重启策略可以在flink-conf.yaml中配置，表示全局的配置。 也可以在应用代码中动态指定，则会覆盖集群的默认策略。

默认的重启策略可以通过 Flink 的配置文件 flink-conf.yaml 配置参数 restart-strategy 定义了哪个策略被使用，常用的重启策略：

- 固定间隔 (Fixed delay)
- 失败率 (Failure rate)
- 无重启 (No restart)

如果没有启用 checkpointing，则使用无重启 (no restart) 策略。 如果启用了 checkpointing，但没有配置重启策略，则使用固定间隔 (fixed-delay) 策略，其中 Integer.MAX_VALUE 参数是尝试重启次数

重启策略之固定间隔 (Fixed delay)

    第一种：全局配置 flink-conf.yaml
        restart-strategy: fixed-delay
        restart-strategy.fixed-delay.attempts: 3
        restart-strategy.fixed-delay.delay: 10 s
    
    第二种：应用代码设置
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
          3, // 尝试重启的次数
          Time.of(10, TimeUnit.SECONDS) // 间隔
        ));

重启策略之失败率 (Failure rate)

    第一种：全局配置 flink-conf.yaml
        restart-strategy: failure-rate
        restart-strategy.failure-rate.max-failures-per-interval: 3
        restart-strategy.failure-rate.failure-rate-interval: 5 min
        restart-strategy.failure-rate.delay: 10 s
    
    第二种：应用代码设置
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
          3, // 一个时间段内的最大失败次数
          Time.of(5, TimeUnit.MINUTES), // 衡量失败次数的是时间段
          Time.of(10, TimeUnit.SECONDS) // 间隔
        ));

重启策略之无重启 (No restart)

第一种：全局配置 flink-conf.yaml
	restart-strategy: none

第二种：应用代码设置
	env.setRestartStrategy(RestartStrategies.noRestart());

保存多个Checkpoint

默认情况下，如果设置了Checkpoint选项，则Flink只保留最近成功生成的1个Checkpoint，而当Flink程序失败时，可以从最近的这个Checkpoint来进行恢复。但是，如果我们希望保留多个Checkpoint，并能够根据实际需要选择其中一个进行恢复，这样会更加灵活，比如，我们发现最近4个小时数据记录处理有问题，希望将整个状态还原到4小时之前

Flink可以支持保留多个Checkpoint，需要在Flink的配置文件conf/flink-conf.yaml中，添加如下配置，指定最多需要保存Checkpoint的个数

    state.checkpoints.num-retained: 20

这样设置以后就查看对应的Checkpoint在HDFS上存储的文件目录

    hdfs dfs -ls hdfs://namenode:9000/flink/checkpoints

如果希望回退到某个Checkpoint点，只需要指定对应的某个Checkpoint路径即可实现

这样设置以后就查看对应的Checkpoint在HDFS上存储的文件目录

如果希望回退到某个Checkpoint点，只需要指定对应的某个Checkpoint路径即可实现

    hdfs dfs -ls hdfs://namenode:9000/flink/checkpoints

如果希望回退到某个Checkpoint点，只需要指定对应的某个Checkpoint路径即可实现

从Checkpoint进行恢复

如果Flink程序异常失败，或者最近一段时间内数据处理错误，我们可以将程序从某一个Checkpoint点进行恢复

    flink run -s hdfs://namenode:9000/flink/checkpoints/467e17d2cc343e6c56255d222bae3421/chk-56/_metadata flink-job.jar

程序正常运行后，还会按照Checkpoint配置进行运行，继续生成Checkpoint数据

savePoint

- Flink通过Savepoint功能可以做到程序升级后，继续从升级前的那个点开始执行计算，保证数据不中断
- 全局，一致性快照。可以保存数据源offset，operator操作状态等信息
- 可以从应用在过去任意做了savepoint的时刻开始继续消费

checkPoint vs savePoint

checkPoint
	应用定时触发，用于保存状态，会过期
	内部应用失败重启的时候使用
savePoint
	用户手动执行，是指向Checkpoint的指针，不会过期
	在升级的情况下使用

注意：为了能够在作业的不同版本之间以及 Flink 的不同版本之间顺利升级，强烈推荐程序员通过 uid(String) 方法手动的给算子赋予 ID，这些 ID 将用于确定每一个算子的状态范围。如果不手动给各算子指定 ID，则会由 Flink 自动给每个算子生成一个 ID。只要这些 ID 没有改变就能从保存点（savepoint）将程序恢复回来。而这些自动生成的 ID 依赖于程序的结构，并且对代码的更改是很敏感的。因此，强烈建议用户手动的设置 ID。

    DataStream<String> stream = env.
    	.addSource(new StatefulSource())	// Statuful source (e.g. kafka) with ID
    	.uid("source-id")		// ID for the source operator
    	.shuffle()
    	.map(new StatefulMapper())		// Stateful mapper with ID
    	.uid("mapper-id")		// ID for the mappper
    	.print();		// Stateless printing sink,Auto-generated ID

savePoint的使用

1. 在flink-conf.yaml中配置Savepoint存储位置
不是必须设置，但是设置后，后面创建指定Job的Savepoint时，可以不用在手动执行命令时指定Savepoint的位置
       state.savepoints.dir: hdfs://namenode:9000/flink/savepoints
2. 触发一个savepoint【直接触发或者在cancel的时候触发】
       bin/flink savepoint jobId [targetDirectory] [-yid yarnAppId]【针对on yarn模式需要指定-yid参数】
       bin/flink cancel -s [targetDirectory] jobId [-yid yarnAppId]【针对on yarn模式需要指定-yid参数】
3. 从指定的savepoint启动job
       bin/flink run -s savepointPath [runArgs]
   
