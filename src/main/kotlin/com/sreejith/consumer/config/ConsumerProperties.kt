package com.sreejith.consumer.config

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Single source of truth for the consumer's topic names and retry tuning.
 *
 * Bound from the `consumer.*` block in application.yml. The retry/DLT values are
 * also referenced from the `@RetryableTopic` annotation via property placeholders
 * (e.g. `attempts = "\${consumer.retry.attempts}"`) so there is exactly one place
 * to change them.
 */
@ConfigurationProperties(prefix = "consumer")
data class ConsumerProperties(
	/** Main topic the listener reads from. */
	val topic: String,
	/** Dead-letter topic that exhausted and non-retryable messages are routed to. */
	val dltTopic: String,
	val retry: Retry,
) {
	data class Retry(
		/** Total delivery attempts for a transient failure: 1 original + (attempts-1) retries. */
		val attempts: Int,
		/** Fixed delay between retry attempts, in milliseconds. */
		val backoffMs: Long,
	)
}
