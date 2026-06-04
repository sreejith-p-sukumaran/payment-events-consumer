package com.sreejith.consumer.service

import com.sreejith.consumer.domain.PaymentAudit
import com.sreejith.consumer.domain.ProcessedEvent
import com.sreejith.consumer.error.NonRetryableException
import com.sreejith.consumer.event.PaymentCompletedEvent
import com.sreejith.consumer.repository.PaymentAuditRepository
import com.sreejith.consumer.repository.ProcessedEventRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Clock

@Service
class IdempotentPaymentProcessor(
	private val processedEventRepository: ProcessedEventRepository,
	private val paymentAuditRepository: PaymentAuditRepository,
	private val clock: Clock,
) {
	private val log = LoggerFactory.getLogger(IdempotentPaymentProcessor::class.java)

	/**
	 * Applies a payment event exactly once. The dedupe check and the side effect
	 * share one transaction: if the event id was already processed we skip; if a
	 * concurrent duplicate races past the check, the processed_event primary key
	 * rejects the insert and the whole transaction (including the audit row) rolls
	 * back — so the effect is applied at most once.
	 *
	 * Input is validated first: a malformed/invalid event throws
	 * [NonRetryableException] before any state is touched, so it never consumes a
	 * retry attempt and is routed straight to the DLT.
	 *
	 * @return true if the event was applied, false if skipped as a duplicate.
	 * @throws NonRetryableException if the event fails business-rule validation.
	 */
	@Transactional
	fun process(event: PaymentCompletedEvent): Boolean {
		validate(event)

		if (processedEventRepository.existsById(event.eventId)) {
			log.debug("Duplicate event {} — already processed, skipping", event.eventId)
			return false
		}

		val now = clock.instant()
		processedEventRepository.save(ProcessedEvent(event.eventId, event.paymentId, now))
		paymentAuditRepository.save(
			PaymentAudit(
				paymentId = event.paymentId,
				amount = event.amount,
				currency = event.currency,
				recordedAt = now,
			),
		)
		log.info("Processed payment event {} for payment {}", event.eventId, event.paymentId)
		return true
	}

	/**
	 * Rejects malformed or business-rule-violating events. These can never succeed
	 * on retry, so they are surfaced as [NonRetryableException] to skip retries.
	 */
	private fun validate(event: PaymentCompletedEvent) {
		if (event.eventId.isBlank()) {
			throw NonRetryableException("eventId must not be blank")
		}
		if (event.paymentId.isBlank()) {
			throw NonRetryableException("paymentId must not be blank")
		}
		if (event.amount.signum() <= 0) {
			throw NonRetryableException("amount must be positive, was ${event.amount}")
		}
		if (!CURRENCY_CODE.matches(event.currency)) {
			throw NonRetryableException("currency must be a 3-letter code, was '${event.currency}'")
		}
	}

	private companion object {
		private val CURRENCY_CODE = Regex("^[A-Z]{3}$")
	}
}
