package com.sreejith.consumer.service

import com.sreejith.consumer.domain.PaymentAudit
import com.sreejith.consumer.domain.ProcessedEvent
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
	 * @return true if the event was applied, false if skipped as a duplicate.
	 */
	@Transactional
	fun process(event: PaymentCompletedEvent): Boolean {
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
}
