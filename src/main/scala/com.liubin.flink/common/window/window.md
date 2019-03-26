Window详解

Window(窗口)

聚合事件（比如计数、求和）在流上的工作方式与批处理不同。比如，对流中的所有元素进行计数是不可能的，因为通常流是无限的（无界的）。所以，流上的聚合需要由 window 来划定范围，比如 “计算过去的5分钟” ，或者 “最后100个元素的和” 。

window是一种可以把无限数据切割为有限数据块的手段，窗口可以是 时间驱动的 【Time Window】（比如：每30秒）或者 数据驱动的【Count Window】 （比如：每100个元素）。

窗口类型

- TimeWindow
- CountWindow
- 自定义Window

窗口通常被区分为不同的类型:

- tumbling windows：滚动窗口 【没有重叠】 
- sliding windows：滑动窗口 【有重叠】
- session windows：会话窗口 

Window增量聚合

- 窗口中每进入一条数据，就进行一次计算
- reduce(reduceFunction)
- aggregate(aggregateFunction)
- sum(),min(),max()

增量聚合状态变化过程——累加求和


Window全量聚合

- 等属于窗口的数据到齐，才开始进行聚合计算【可以实现对窗口内的数据进行排序等需求】
- apply(windowFunction)
- process(processWindowFunction)
- processWindowFunction比windowFunction提供了更多的上下文信息。


