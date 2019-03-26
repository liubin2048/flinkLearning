package com.liubin.flink.common.stateManagement

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * author : liubin
  * date : 2019/3/11
  * Description :
  *  Flink支持不同的重启策略，以在故障发生时控制作业如何重启
  * 集群在启动时会伴随一个默认的重启策略，在没有定义具体重启策略时会使用该默认策略。
  * 重启策略可以在flink-conf.yaml中配置，表示全局的配置。 也可以在应用代码中动态指定，则会覆盖集群的默认策略。
  *
  * 常用的重启策略：
  *   - 固定间隔 (Fixed delay)
  *   - 失败率 (Failure rate)
  *   - 不重启 (No restart)
  *
  * 如果没有启用 checkpointing，则使用无重启 (no restart) 策略。
  * 如果启用了 checkpointing，但没有配置重启策略，则使用固定间隔 (fixed-delay) 策略
  */

object RestartStrateDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 全局配置 flink-conf.yaml
    /*
      1.固定间隔 (Fixed delay)
        restart-strategy: fixed-delay
        restart-strategy.fixed-delay.attempts: 3
        restart-strategy.fixed-delay.delay: 10 s

      2.失败率 (Failure rate)
        restart-strategy: failure-rate
        restart-strategy.failure-rate.max-failures-per-interval: 3
        restart-strategy.failure-rate.failure-rate-interval: 5 min
        restart-strategy.failure-rate.delay: 10 s

      3.不重启 (No restart)
        restart-strategy: none
    */


    // 应用代码设置

    // 1.固定间隔 (Fixed delay)
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        3, // 尝试重启的次数
        Time.seconds(10) // 间隔
      )
    )

    // 2.失败率 (Failure rate)
    env.setRestartStrategy(
      RestartStrategies.failureRateRestart(
        3, // 一个时间段内的最大失败次数
        Time.minutes(5), // 衡量失败次数的时间段
        Time.seconds(10) // 间隔
      )
    )

    // 3.不重启 (No restart)
    env.setRestartStrategy(RestartStrategies.noRestart())

  }
}
