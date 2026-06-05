package com.sreejith.consumer.dlq

import com.fasterxml.jackson.databind.ObjectMapper
import com.ninjasquad.springmockk.MockkBean
import com.sreejith.consumer.error.NonRetryableException
import com.sreejith.consumer.event.PaymentCompletedEvent
import com.sreejith.consumer.service.IdempotentPaymentProcessor
import io.mockk.every
import io.mockk.verify
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.Properties

/**
 * Retry / DLQ / replay behaviour over a real Kafka broker (Testcontainers). The
 * processor is mocked so each scenario can drive a precise failure sequence; the
 * idempotency/DB behaviour is covered separately by the processor and e2e tests.
 *
 * Scenarios: (a) transient failure retried then succeeds; (b) non-retryable goes
 * straight to the DLT; (c) retries are exhausted into the DLT; (d) a dead-lettered
 * record is replayed and reprocessed once the cause is fixed.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class RetryAndDlqFlowTest(
	@Autowired private val objectMapper: ObjectMapper,
	@Autowired private val inspector: DlqInspector,
	@Autowired private val replayer: DlqReplayer,
) {

	@MockkBean
	private lateinit var processor: IdempotentPaymentProcessor

	companion object {
		@Container
		@JvmStatic
		val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4")
			.withDatabaseName("payments_consumer")
			.withUsername("consumer")
			.withPassword("consumer")

		@Container
		@JvmStatic
		val kafka: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))

		@DynamicPropertySource
		@JvmStatic
		fun props(registry: DynamicPropertyRegistry) {
			registry.add("spring.datasource.url", mysql::getJdbcUrl)
			registry.add("spring.datasource.username", mysql::getUsername)
			registry.add("spring.datasource.password", mysql::getPassword)
			registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
		}
	}

	private lateinit var producer: Producer<String, String>

	@BeforeEach
	fun setUp() {
		val props = Properties().apply {
			put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
			put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
			put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
		}
		producer = KafkaProducer(props)
	}

	@AfterEach
	fun tearDown() {
		producer.close()
	}

	@Test
	fun `a transient failure is retried and eventually succeeds without dead-lettering`() {
		// Fail the first two attempts, succeed on the third (within attempts = 3).
		every { processor.process(any()) } throws transient() andThenThrows transient() andThen true
		val event = event("evt-a", "pay-a")

		publish(event)

		await().atMost(Duration.ofSeconds(30)).untilAsserted {
			verify(exactly = 3) { processor.process(any()) }
		}
		// Succeeded on the retry, so it was never dead-lettered.
		assertThat(inspector.inspect().none { it.payload == json(event) }).isTrue()
	}

	@Test
	fun `a non-retryable failure skips retries and goes straight to the DLT`() {
		every { processor.process(any()) } throws NonRetryableException("invalid by rule")
		val event = event("evt-b", "pay-b")

		publish(event)

		val dead = awaitDeadLetter(json(event))
		assertThat(dead.attempts).isEqualTo(1) // no retry attempts consumed
		assertThat(dead.exceptionMessage).isEqualTo("invalid by rule")
		verify(exactly = 1) { processor.process(any()) }
	}

	@Test
	fun `a record that exhausts all retries lands in the DLT`() {
		every { processor.process(any()) } throws transient()
		val event = event("evt-c", "pay-c")

		publish(event)

		val dead = awaitDeadLetter(json(event))
		assertThat(dead.originalTopic).isEqualTo("payment-events")
		assertThat(dead.exceptionMessage).isEqualTo("transient downstream failure")
		verify(exactly = 3) { processor.process(any()) } // original + 2 retries
	}

	@Test
	fun `a dead-lettered record is reprocessed after replay once the cause is fixed`() {
		every { processor.process(any()) } throws transient()
		val event = event("evt-d", "pay-d")

		publish(event)
		val dead = awaitDeadLetter(json(event))
		verify(exactly = 3) { processor.process(any()) }

		// The cause is fixed: processing now succeeds. Replay the dead-lettered record.
		every { processor.process(any()) } returns true
		replayer.replay(dead)

		await().atMost(Duration.ofSeconds(30)).untilAsserted {
			verify(atLeast = 4) { processor.process(any()) } // 3 from exhaustion + the replay
		}
		// The replayed delivery succeeded, so no new dead-letter for this event.
		assertThat(inspector.inspect().count { it.payload == json(event) }).isEqualTo(1)
	}

	private fun transient() = IllegalStateException("transient downstream failure")

	private fun event(eventId: String, paymentId: String) = PaymentCompletedEvent(
		eventId = eventId,
		paymentId = paymentId,
		amount = BigDecimal("10.00"),
		currency = "EUR",
		occurredAt = Instant.parse("2026-06-02T12:00:00Z"),
	)

	private fun json(event: PaymentCompletedEvent) = objectMapper.writeValueAsString(event)

	private fun publish(event: PaymentCompletedEvent) {
		producer.send(ProducerRecord("payment-events", event.paymentId, json(event)))
		producer.flush()
	}

	private fun awaitDeadLetter(payload: String): DeadLetterRecord {
		await().atMost(Duration.ofSeconds(30)).untilAsserted {
			assertThat(inspector.inspect().map { it.payload }).contains(payload)
		}
		return inspector.inspect().first { it.payload == payload }
	}
}
