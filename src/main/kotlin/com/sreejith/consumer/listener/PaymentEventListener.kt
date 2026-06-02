package com.sreejith.consumer.listener

import com.fasterxml.jackson.databind.ObjectMapper
import com.sreejith.consumer.event.PaymentCompletedEvent
import com.sreejith.consumer.service.IdempotentPaymentProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class PaymentEventListener(
	private val processor: IdempotentPaymentProcessor,
	private val objectMapper: ObjectMapper,
) {
	private val log = LoggerFactory.getLogger(PaymentEventListener::class.java)

	@KafkaListener(
		topics = ["\${consumer.topic}"],
		groupId = "\${spring.kafka.consumer.group-id}",
	)
	fun onMessage(record: ConsumerRecord<String, String>) {
		val event = objectMapper.readValue(record.value(), PaymentCompletedEvent::class.java)
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
