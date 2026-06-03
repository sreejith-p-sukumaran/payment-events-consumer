package com.sreejith.consumer.e2e

import com.fasterxml.jackson.databind.ObjectMapper
import com.sreejith.consumer.event.PaymentCompletedEvent
import com.sreejith.consumer.repository.PaymentAuditRepository
import com.sreejith.consumer.repository.ProcessedEventRepository
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
 * End-to-end over real MySQL and a real Kafka broker (both Testcontainers): the
 * consumer's half of the flow. A PaymentCompleted event delivered twice on the
 * topic is processed exactly once. This meets the producer-side e2e (in the
 * outbox-payments-service repo) at the `payment-events` topic.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class PaymentConsumerEndToEndTest(
	@Autowired private val processedEventRepository: ProcessedEventRepository,
	@Autowired private val paymentAuditRepository: PaymentAuditRepository,
	@Autowired private val objectMapper: ObjectMapper,
) {
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
		paymentAuditRepository.deleteAll()
		processedEventRepository.deleteAll()
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
	fun `an event delivered twice over a real broker is processed once`() {
		val payment = PaymentCompletedEvent(
			eventId = "e2e-evt-1",
			paymentId = "e2e-pay-1",
			amount = BigDecimal("63.00"),
			currency = "EUR",
			occurredAt = Instant.parse("2026-06-02T15:00:00Z"),
		)
		val fence = payment.copy(eventId = "e2e-fence", paymentId = "e2e-fence")

		publish(payment)
		publish(payment) // duplicate
		publish(fence)
		producer.flush()

		await().atMost(Duration.ofSeconds(30)).untilAsserted {
			assertThat(processedEventRepository.existsById("e2e-fence")).isTrue()
		}

		assertThat(paymentAuditRepository.countByPaymentId("e2e-pay-1")).isEqualTo(1)
		assertThat(processedEventRepository.count()).isEqualTo(2) // event + fence
	}

	private fun publish(event: PaymentCompletedEvent) {
		producer.send(
			ProducerRecord("payment-events", event.paymentId, objectMapper.writeValueAsString(event)),
		)
	}
}
