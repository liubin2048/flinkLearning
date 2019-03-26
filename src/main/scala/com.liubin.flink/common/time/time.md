Time介绍

针对stream数据中的时间，可以分为以下三种

- Event Time：事件产生的时间，它通常由事件中的时间戳描述。
- Ingestion time：事件进入Flink的时间
- Processing Time：事件被处理时当前系统的时间



Time例子分析

原始日志如下
	2018-10-10 10:00:01,134 INFO executor.Executor: Finished task in state 0.0
	这条数据进入Flink的时间是2018-10-10 20:00:00,102
	到达window处理的时间为2018-10-10 20:00:01,100 
如果我们想要统计每分钟内接口调用失败的错误日志个数，使用哪个时间才有意义？

Flink中，默认Time类似是ProcessingTime（可以在代码中设置）

EventTime和Watermarks

在使用eventTime的时候如何处理乱序数据？

我们知道，流处理从事件产生，到流经source，再到operator，中间是有一个过程和时间的。虽然大部分情况下，流到operator的数据都是按照事件产生的时间顺序来的，但是也不排除由于网络延迟等原因，导致乱序的产生，特别是使用kafka的话，多个分区的数据无法保证有序。所以在进行window计算的时候，我们又不能无限期的等下去，必须要有个机制来保证一个特定的时间后，必须触发window去进行计算了。这个特别的机制，就是watermark，watermark是用于处理乱序事件的。watermark可以翻译为水位线

- 有序的流的watermarks

- 无序的流的watermarks

- 多并行度流的watermarks

注意：多并行度的情况下，watermark对齐会取所有channel最小的watermark



watermarks生成方式

通常，在接收到source的数据后，应该立刻生成watermark；但是，也可以在source后，应用简单的map或者filter操作后，再生成watermark。注意：如果指定多次watermark，后面指定的会覆盖前面的值。

生成方式

- With Periodic Watermarks
  - 周期性的触发watermark的生成和发送，默认是100ms
  - 每隔N秒自动向流里注入一个WATERMARK 时间间隔由ExecutionConfig.setAutoWatermarkInterval 决定. 每次调用getCurrentWatermark 方法, 如果得到的WATERMARK 不为空并且比之前的大就注入流中 
  - 可以定义一个最大允许乱序的时间，这种比较常用
  - 实现AssignerWithPeriodicWatermarks接口
- With Punctuated Watermarks
  - 基于某些事件触发watermark的生成和发送
  - 基于事件向流里注入一个WATERMARK，每一个元素都有机会判断是否生成一个WATERMARK. 如果得到的WATERMARK 不为空并且比之前的大就注入流中
  - 实现AssignerWithPunctuatedWatermarks接口

With Periodic Watermarks案例

详细分析过程见<<EventTime和Watermarks案例分析.doc>>

Flink应该如何设置最大乱序时间

- 这个要结合自己的业务以及数据情况去设置。如果maxOutOfOrderness设置的太小，而自身数据发送时由于网络等原因导致乱序或者late太多，那么最终的结果就是会有很多单条的数据在window中被触发，数据的正确性影响太大
- 对于严重乱序的数据，需要严格统计数据最大延迟时间，才能保证计算的数据准确，延时设置太小会影响数据准确性，延时设置太大不仅影响数据的实时性，更加会加重Flink作业的负担，不是对eventTime要求特别严格的数据，尽量不要采用eventTime方式来处理，会有丢数据的风险。
