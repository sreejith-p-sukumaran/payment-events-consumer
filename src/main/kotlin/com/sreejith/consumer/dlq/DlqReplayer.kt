package com.sreejith.consumer.dlq

import com.sreejith.consumer.config.ConsumerProperties
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

/**
 * Replays dead-lettered messages back onto the main topic after the underlying
 * cause has been fixed. The original payload and key are preserved, so the message
 * lands on the same partition and the consumer's idempotency guard (keyed on event
 * id) ensures a replay never double-applies an effect that already succeeded.
 *
 * Replay does not delete the record from the DLT (Kafka topics are append-only);
 * the source-of-truth is offset progression, and re-inspecting after a fix shows
 * what is left to handle.
 */
@Component
class DlqReplayer(
	private val kafkaTemplate: KafkaTemplate<String, String>,
	private val properties: ConsumerProperties,
) {
	private val log = LoggerFactory.getLogger(DlqReplayer::class.java)

	/** Republishes a single dead-lettered record to the topic it originally came from. */
	fun replay(record: DeadLetterRecord) {
		val target = record.originalTopic.ifBlank { properties.topic }
		kafkaTemplate.send(ProducerRecord(target, record.key, record.payload))
		kafkaTemplate.flush()
		log.info(
			"Replayed dead-letter record (key={}, dltOffset={}) back to {}",
			record.key, record.offset, target,
		)
	}

	/** Republishes a batch of dead-lettered records. */
	fun replayAll(records: List<DeadLetterRecord>) = records.forEach(::replay)
}
