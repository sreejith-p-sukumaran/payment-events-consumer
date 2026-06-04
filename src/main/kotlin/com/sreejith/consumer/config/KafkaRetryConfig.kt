package com.sreejith.consumer.config

import com.sreejith.consumer.dlq.DltHeaders
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport
import org.springframework.kafka.retrytopic.RetryTopicHeaders
import org.springframework.kafka.retrytopic.RetryTopicSchedulerWrapper
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import java.nio.ByteBuffer
import java.time.Clock

/**
 * Bootstraps and globally configures Spring Kafka's non-blocking retry-topic
 * infrastructure (used via `@RetryableTopic` on the listener). Extending
 * [RetryTopicConfigurationSupport] is the documented global-customization hook;
 * it replaces `@EnableKafkaRetryTopic` (the two must not be combined).
 *
 * The infrastructure forwards records to `payment-events-retry-*` and finally to
 * the dead-letter topic, where the [DeadLetterPublishingRecoverer] enriches each
 * record. We add app-owned [DltHeaders] so the DLQ is self-describing for the
 * inspector/replay path.
 */
@Configuration
class KafkaRetryConfig(private val clock: Clock) : RetryTopicConfigurationSupport() {

	/**
	 * Non-blocking retry schedules each delayed re-delivery on a TaskScheduler.
	 * We expose a dedicated one wrapped in [RetryTopicSchedulerWrapper] so the
	 * framework uses *this* scheduler rather than an arbitrary `TaskScheduler` bean.
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

	override fun configureCustomizers(customizersConfigurer: CustomizersConfigurer) {
		// Append our headers; Spring's standard kafka_dlt-* headers are kept.
		customizersConfigurer.customizeDeadLetterPublishingRecoverer { recoverer: DeadLetterPublishingRecoverer ->
			recoverer.addHeadersFunction { record, exception -> failureContext(record, exception) }
		}
	}

	private fun failureContext(record: ConsumerRecord<*, *>, exception: Exception): Headers {
		val message = rootCause(exception).let { it.message ?: it.javaClass.name }
		return RecordHeaders().apply {
			add(RecordHeader(DltHeaders.EXCEPTION_MESSAGE, message.toByteArray()))
			add(RecordHeader(DltHeaders.ATTEMPTS, attemptsOf(record).toString().toByteArray()))
			add(RecordHeader(DltHeaders.FAILED_AT, clock.instant().toString().toByteArray()))
			add(RecordHeader(DltHeaders.ORIGINAL_TOPIC, record.topic().toByteArray()))
		}
	}

	/**
	 * The framework stamps a retry-attempts header as a record moves through the
	 * retry topics. A non-retryable record routed straight from the main topic has
	 * no such header — that is attempt 1.
	 */
	private fun attemptsOf(record: ConsumerRecord<*, *>): Int {
		val header = record.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS) ?: return 1
		val bytes = header.value()
		return when (bytes.size) {
			Int.SIZE_BYTES -> ByteBuffer.wrap(bytes).int
			else -> String(bytes).trim().toIntOrNull() ?: 1
		}
	}

	private fun rootCause(throwable: Throwable): Throwable {
		var current = throwable
		while (current.cause != null && current.cause !== current) {
			current = current.cause as Throwable
		}
		return current
	}
}
