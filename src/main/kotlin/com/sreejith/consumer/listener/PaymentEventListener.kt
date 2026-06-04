package com.sreejith.consumer.listener

import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.databind.ObjectMapper
import com.sreejith.consumer.error.NonRetryableException
import com.sreejith.consumer.event.PaymentCompletedEvent
import com.sreejith.consumer.service.IdempotentPaymentProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.retry.annotation.Backoff
import org.springframework.stereotype.Component

@Component
class PaymentEventListener(
	private val processor: IdempotentPaymentProcessor,
	private val objectMapper: ObjectMapper,
) {
	private val log = LoggerFactory.getLogger(PaymentEventListener::class.java)

	/**
	 * Non-blocking retry: when processing throws a *transient* error, the record is
	 * forwarded to a retry topic stamped with a due-time and the main partition keeps
	 * flowing — we never `Thread.sleep` the consumer thread. A dedicated retry-topic
	 * consumer waits out the backoff and re-delivers. After [consumer.retry.attempts]
	 * total attempts the record is routed to the dead-letter topic.
	 *
	 * `exclude = NonRetryableException` makes malformed/invalid events skip the retry
	 * topics entirely and go straight to the DLT — we don't waste attempts on input
	 * that can never succeed.
	 *
	 * Ordering trade-off: moving a failed record onto a separate retry topic means
	 * later records for the SAME key on the main partition can be processed before
	 * the failed one is retried. We accept relaxed per-key ordering in exchange for
	 * head-of-line-blocking-free throughput; the processor stays idempotent so a
	 * re-applied or out-of-order retry causes no double effect.
	 */
	@RetryableTopic(
		attempts = "\${consumer.retry.attempts}",
		backoff = Backoff(delayExpression = "\${consumer.retry.backoff-ms}"),
		exclude = [NonRetryableException::class],
		// Default suffixes resolve to payment-events-retry-* and payment-events-dlt,
		// the latter matching `consumer.dlt-topic` used by the DLQ inspector/replay.
	)
	@KafkaListener(
		topics = ["\${consumer.topic}"],
		groupId = "\${spring.kafka.consumer.group-id}",
	)
	fun onMessage(record: ConsumerRecord<String, String>) {
		// A payload that cannot be parsed will never parse on retry — classify it
		// as non-retryable so it skips the retry topics and goes straight to the DLT.
		val event = try {
			objectMapper.readValue(record.value(), PaymentCompletedEvent::class.java)
		} catch (ex: JacksonException) {
			throw NonRetryableException(
				"Malformed payment event payload at ${record.topic()}-${record.partition()}@${record.offset()}",
				ex,
			)
		}
		try {
			processor.process(event)
		} catch (ex: DataIntegrityViolationException) {
			// A concurrent duplicate beat the exists-check and the processed_event
			// primary key rejected it. The other delivery applied the effect, so
			// treating this as a successful skip is correct.
			log.debug("Duplicate event {} rejected by primary key — skipping", event.eventId, ex)
		}
	}
}
