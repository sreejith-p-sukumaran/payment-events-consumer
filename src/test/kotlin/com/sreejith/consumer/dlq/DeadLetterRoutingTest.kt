package com.sreejith.consumer.dlq

import com.fasterxml.jackson.databind.ObjectMapper
import com.sreejith.consumer.event.PaymentCompletedEvent
import com.sreejith.consumer.repository.PaymentAuditRepository
import com.sreejith.consumer.repository.ProcessedEventRepository
import com.sreejith.consumer.support.AbstractMySqlIntegrationTest
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant

/**
 * A non-retryable event (fails business-rule validation) must skip the retry
 * topics and land on the dead-letter topic directly, carrying its original
 * payload as the value and the app-owned failure-context headers.
 */
@EmbeddedKafka(partitions = 1, topics = ["payment-events", "payment-events-dlt"])
@SpringBootTest(
	webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = ["spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}"],
)
class DeadLetterRoutingTest(
	@Autowired private val objectMapper: ObjectMapper,
	@Autowired private val embeddedKafka: EmbeddedKafkaBroker,
	@Autowired private val processedEventRepository: ProcessedEventRepository,
	@Autowired private val paymentAuditRepository: PaymentAuditRepository,
) : AbstractMySqlIntegrationTest() {

	private lateinit var producer: Producer<String, String>
	private lateinit var dltConsumer: Consumer<String, String>

	@BeforeEach
	fun setUp() {
		paymentAuditRepository.deleteAll()
		processedEventRepository.deleteAll()
		producer = DefaultKafkaProducerFactory(
			KafkaTestUtils.producerProps(embeddedKafka),
			StringSerializer(),
			StringSerializer(),
		).createProducer()
		val consumerProps = KafkaTestUtils.consumerProps("dlt-test-group", "true", embeddedKafka)
		dltConsumer = DefaultKafkaConsumerFactory(consumerProps, StringDeserializer(), StringDeserializer())
			.createConsumer()
		dltConsumer.subscribe(listOf("payment-events-dlt"))
	}

	@AfterEach
	fun tearDown() {
		producer.close()
		dltConsumer.close()
	}

	@Test
	fun `a non-retryable event is routed straight to the dead-letter topic with failure context`() {
		val invalid = PaymentCompletedEvent(
			eventId = "evt-dlt-1",
			paymentId = "pay-dlt-1",
			amount = BigDecimal("-5.00"), // non-positive — NonRetryableException
			currency = "EUR",
			occurredAt = Instant.parse("2026-06-02T12:00:00Z"),
		)
		val payload = objectMapper.writeValueAsString(invalid)

		producer.send(ProducerRecord("payment-events", invalid.paymentId, payload))
		producer.flush()

		val dltRecord = KafkaTestUtils.getSingleRecord(dltConsumer, "payment-events-dlt", Duration.ofSeconds(20))

		// Original payload is preserved as the DLT record value.
		assertThat(dltRecord.value()).isEqualTo(payload)

		// App-owned failure-context headers.
		assertThat(headerValue(dltRecord, DltHeaders.EXCEPTION_MESSAGE)).contains("amount must be positive")
		assertThat(headerValue(dltRecord, DltHeaders.ATTEMPTS)).isEqualTo("1") // straight to DLT, no retries used
		assertThat(headerValue(dltRecord, DltHeaders.ORIGINAL_TOPIC)).isEqualTo("payment-events")
		assertThat(headerValue(dltRecord, DltHeaders.FAILED_AT)).isNotBlank()

		// No side effect was applied for the invalid event.
		assertThat(paymentAuditRepository.count()).isZero()
		assertThat(processedEventRepository.count()).isZero()
	}

	private fun headerValue(record: org.apache.kafka.clients.consumer.ConsumerRecord<String, String>, name: String) =
		record.headers().lastHeader(name)?.let { String(it.value()) }
}
