package com.sreejith.consumer.dlq

import com.sreejith.consumer.config.ConsumerProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant

/**
 * Reads the contents of the dead-letter topic without disturbing the live
 * consumer group: it spins up a throwaway consumer on its own group, assigns all
 * partitions, seeks to the beginning, and drains what is currently there. Offsets
 * are never committed, so inspecting is side-effect free and repeatable.
 */
@Component
class DlqInspector(
	private val consumerFactory: ConsumerFactory<String, String>,
	private val properties: ConsumerProperties,
) {

	/** Snapshot of up to [max] records currently on the dead-letter topic. */
	fun inspect(max: Int = DEFAULT_MAX): List<DeadLetterRecord> {
		consumerFactory.createConsumer(INSPECT_GROUP, "-inspect").use { consumer ->
			val partitions = consumer.partitionsFor(properties.dltTopic)
				?.map { TopicPartition(properties.dltTopic, it.partition()) }
				?: return emptyList()
			if (partitions.isEmpty()) return emptyList()

			consumer.assign(partitions)
			consumer.seekToBeginning(partitions)

			val out = ArrayList<DeadLetterRecord>()
			var emptyPolls = 0
			// Tolerate the initial empty fetch; stop once polling goes quiet or we hit max.
			while (out.size < max && emptyPolls < MAX_EMPTY_POLLS) {
				val polled = consumer.poll(POLL_TIMEOUT)
				if (polled.isEmpty) {
					emptyPolls++
					continue
				}
				emptyPolls = 0
				for (record in polled) {
					out += toDeadLetterRecord(record)
					if (out.size >= max) break
				}
			}
			return out
		}
	}

	private fun toDeadLetterRecord(record: ConsumerRecord<String, String>) = DeadLetterRecord(
		key = record.key(),
		payload = record.value(),
		originalTopic = header(record, DltHeaders.ORIGINAL_TOPIC) ?: "",
		exceptionMessage = header(record, DltHeaders.EXCEPTION_MESSAGE),
		attempts = header(record, DltHeaders.ATTEMPTS)?.toIntOrNull(),
		failedAt = header(record, DltHeaders.FAILED_AT)?.let { runCatching { Instant.parse(it) }.getOrNull() },
		partition = record.partition(),
		offset = record.offset(),
	)

	private fun header(record: ConsumerRecord<String, String>, name: String): String? =
		record.headers().lastHeader(name)?.let { String(it.value()) }

	private companion object {
		private const val DEFAULT_MAX = 100
		private const val MAX_EMPTY_POLLS = 2
		private const val INSPECT_GROUP = "payment-events-dlq-inspector"
		private val POLL_TIMEOUT = Duration.ofMillis(500)
	}
}
