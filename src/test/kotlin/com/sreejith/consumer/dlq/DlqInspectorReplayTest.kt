package com.sreejith.consumer.dlq

import com.fasterxml.jackson.databind.ObjectMapper
import com.sreejith.consumer.event.PaymentCompletedEvent
import com.sreejith.consumer.repository.PaymentAuditRepository
import com.sreejith.consumer.repository.ProcessedEventRepository
import com.sreejith.consumer.support.AbstractMySqlIntegrationTest
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant

/**
 * Covers the inspector and replay path in isolation: a dead-lettered (but now
 * valid) message is seeded on the DLT, read back by the inspector with its failure
 * context, then replayed to the main topic where the consumer applies it exactly
 * once.
 */
@EmbeddedKafka(partitions = 1, topics = ["payment-events", "payment-events-dlt"])
@SpringBootTest(
	webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = ["spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}"],
)
class DlqInspectorReplayTest(
	@Autowired private val inspector: DlqInspector,
	@Autowired private val replayer: DlqReplayer,
	@Autowired private val objectMapper: ObjectMapper,
	@Autowired private val embeddedKafka: EmbeddedKafkaBroker,
	@Autowired private val processedEventRepository: ProcessedEventRepository,
	@Autowired private val paymentAuditRepository: PaymentAuditRepository,
) : AbstractMySqlIntegrationTest() {

	private lateinit var producer: Producer<String, String>

	@BeforeEach
	fun setUp() {
		paymentAuditRepository.deleteAll()
		processedEventRepository.deleteAll()
		producer = DefaultKafkaProducerFactory(
			KafkaTestUtils.producerProps(embeddedKafka),
			StringSerializer(),
			StringSerializer(),
		).createProducer()
	}

	@AfterEach
	fun tearDown() {
		producer.close()
	}

	@Test
	fun `a dead-lettered message can be inspected and replayed to the main topic`() {
		val event = PaymentCompletedEvent(
			eventId = "evt-replay-1",
			paymentId = "pay-replay-1",
			amount = BigDecimal("99.00"),
			currency = "EUR",
			occurredAt = Instant.parse("2026-06-02T12:00:00Z"),
		)
		val payload = objectMapper.writeValueAsString(event)
		seedDeadLetter(event.paymentId, payload)

		// Inspect: the seeded record is read back with its failure context. The DLT
		// is append-only and shared across tests, so target our record by payload
		// rather than assuming the topic is empty.
		await().atMost(Duration.ofSeconds(20)).untilAsserted {
			assertThat(inspector.inspect().map { it.payload }).contains(payload)
		}
		val dead = inspector.inspect().first { it.payload == payload }
		assertThat(dead.payload).isEqualTo(payload)
		assertThat(dead.originalTopic).isEqualTo("payment-events")
		assertThat(dead.attempts).isEqualTo(3)
		assertThat(dead.exceptionMessage).isEqualTo("transient downstream failure")

		// Replay: republished to the main topic and applied exactly once.
		replayer.replay(dead)

		await().atMost(Duration.ofSeconds(20)).untilAsserted {
			assertThat(processedEventRepository.existsById("evt-replay-1")).isTrue()
		}
		assertThat(paymentAuditRepository.countByPaymentId("pay-replay-1")).isEqualTo(1)
	}

	/** Writes a record onto the DLT with the same headers the recoverer would add. */
	private fun seedDeadLetter(key: String, payload: String) {
		val headers = listOf(
			RecordHeader(DltHeaders.ORIGINAL_TOPIC, "payment-events".toByteArray()),
			RecordHeader(DltHeaders.EXCEPTION_MESSAGE, "transient downstream failure".toByteArray()),
			RecordHeader(DltHeaders.ATTEMPTS, "3".toByteArray()),
			RecordHeader(DltHeaders.FAILED_AT, Instant.parse("2026-06-02T12:00:05Z").toString().toByteArray()),
		)
		producer.send(ProducerRecord("payment-events-dlt", null, key, payload, headers))
		producer.flush()
	}
}
