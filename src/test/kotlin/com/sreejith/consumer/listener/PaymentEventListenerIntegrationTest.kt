package com.sreejith.consumer.listener

import com.fasterxml.jackson.databind.ObjectMapper
import com.sreejith.consumer.event.PaymentCompletedEvent
import com.sreejith.consumer.repository.PaymentAuditRepository
import com.sreejith.consumer.repository.ProcessedEventRepository
import com.sreejith.consumer.support.AbstractMySqlIntegrationTest
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
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
 * Drives the consumer through a real (embedded) Kafka topic: a duplicate event
 * is delivered twice but applied once. A distinct "fence" event sent last lets
 * the test wait deterministically until all three records have been consumed,
 * so the duplicate has provably been seen and skipped.
 */
@EmbeddedKafka(partitions = 3, topics = ["payment-events"])
@SpringBootTest(
	webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = ["spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}"],
)
class PaymentEventListenerIntegrationTest(
	@Autowired private val processedEventRepository: ProcessedEventRepository,
	@Autowired private val paymentAuditRepository: PaymentAuditRepository,
	@Autowired private val objectMapper: ObjectMapper,
	@Autowired private val embeddedKafka: EmbeddedKafkaBroker,
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
	fun `a duplicate event delivered twice is applied once`() {
		val payment = PaymentCompletedEvent(
			eventId = "evt-100",
			paymentId = "pay-100",
			amount = BigDecimal("42.00"),
			currency = "EUR",
			occurredAt = Instant.parse("2026-06-02T12:00:00Z"),
		)
		val fence = payment.copy(eventId = "evt-fence", paymentId = "pay-fence")

		publish(payment) // first delivery
		publish(payment) // duplicate delivery
		publish(fence) // fence — distinct event processed last
		producer.flush()

		// Once the fence is recorded, all earlier records have been consumed.
		await().atMost(Duration.ofSeconds(20)).untilAsserted {
			assertThat(processedEventRepository.existsById("evt-fence")).isTrue()
		}

		assertThat(paymentAuditRepository.countByPaymentId("pay-100")).isEqualTo(1)
		assertThat(processedEventRepository.existsById("evt-100")).isTrue()
		assertThat(processedEventRepository.count()).isEqualTo(2) // evt-100 + evt-fence
	}

	private fun publish(event: PaymentCompletedEvent) {
		producer.send(
			ProducerRecord("payment-events", event.paymentId, objectMapper.writeValueAsString(event)),
		)
	}
}
