package com.sreejith.consumer.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaRetryTopic
import org.springframework.kafka.retrytopic.RetryTopicSchedulerWrapper
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler

/**
 * Bootstraps Spring Kafka's non-blocking retry-topic infrastructure so that
 * `@RetryableTopic` on the listener takes effect. This wires up the retry-topic
 * consumers and the [org.springframework.kafka.listener.DeadLetterPublishingRecoverer]
 * that forwards records to the retry topics and, finally, the dead-letter topic.
 *
 * Requires a `KafkaTemplate` bean (auto-configured from `spring.kafka.producer.*`).
 */
@Configuration
@EnableKafkaRetryTopic
class KafkaRetryConfig {

	/**
	 * Non-blocking retry schedules each delayed re-delivery on a [org.springframework.scheduling.TaskScheduler].
	 * We expose a dedicated one wrapped in [RetryTopicSchedulerWrapper] so the framework
	 * uses *this* scheduler rather than picking up an arbitrary `TaskScheduler` bean.
	 */
	@Bean
	fun retryTopicSchedulerWrapper(): RetryTopicSchedulerWrapper {
		val scheduler = ThreadPoolTaskScheduler().apply {
			poolSize = 1
			setThreadNamePrefix("kafka-retry-")
			initialize()
		}
		return RetryTopicSchedulerWrapper(scheduler)
	}
}
