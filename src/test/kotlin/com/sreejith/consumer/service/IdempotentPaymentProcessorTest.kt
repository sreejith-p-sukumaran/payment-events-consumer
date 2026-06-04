package com.sreejith.consumer.service

import com.sreejith.consumer.error.NonRetryableException
import com.sreejith.consumer.event.PaymentCompletedEvent
import com.sreejith.consumer.repository.PaymentAuditRepository
import com.sreejith.consumer.repository.ProcessedEventRepository
import com.sreejith.consumer.support.AbstractMySqlIntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.math.BigDecimal
import java.time.Instant

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class IdempotentPaymentProcessorTest(
	@Autowired private val processor: IdempotentPaymentProcessor,
	@Autowired private val processedEventRepository: ProcessedEventRepository,
	@Autowired private val paymentAuditRepository: PaymentAuditRepository,
) : AbstractMySqlIntegrationTest() {

	@BeforeEach
	fun clean() {
		paymentAuditRepository.deleteAll()
		processedEventRepository.deleteAll()
	}

	@Test
	fun `processing the same event twice produces exactly one effect`() {
		val event = PaymentCompletedEvent(
			eventId = "evt-1",
			paymentId = "pay-1",
			amount = BigDecimal("20.00"),
			currency = "EUR",
			occurredAt = Instant.parse("2026-06-02T10:00:00Z"),
		)

		val firstApplied = processor.process(event)
		val secondApplied = processor.process(event)

		assertThat(firstApplied).isTrue()
		assertThat(secondApplied).isFalse()
		assertThat(processedEventRepository.count()).isEqualTo(1)
		assertThat(paymentAuditRepository.countByPaymentId("pay-1")).isEqualTo(1)
	}

	@Test
	fun `distinct events each produce their own effect`() {
		val base = PaymentCompletedEvent(
			eventId = "evt-a",
			paymentId = "pay-2",
			amount = BigDecimal("5.00"),
			currency = "USD",
			occurredAt = Instant.parse("2026-06-02T10:00:00Z"),
		)

		processor.process(base)
		processor.process(base.copy(eventId = "evt-b"))

		assertThat(processedEventRepository.count()).isEqualTo(2)
		assertThat(paymentAuditRepository.countByPaymentId("pay-2")).isEqualTo(2)
	}

	@Test
	fun `an invalid event is rejected as non-retryable and writes nothing`() {
		val invalid = PaymentCompletedEvent(
			eventId = "evt-bad",
			paymentId = "pay-bad",
			amount = BigDecimal("-1.00"), // non-positive amount — a business-rule violation
			currency = "EUR",
			occurredAt = Instant.parse("2026-06-02T10:00:00Z"),
		)

		assertThatThrownBy { processor.process(invalid) }
			.isInstanceOf(NonRetryableException::class.java)
			.hasMessageContaining("amount must be positive")

		// Validation runs before any side effect, so nothing is persisted.
		assertThat(processedEventRepository.count()).isZero()
		assertThat(paymentAuditRepository.count()).isZero()
	}

	@Test
	fun `a malformed currency is rejected as non-retryable`() {
		val invalid = PaymentCompletedEvent(
			eventId = "evt-cur",
			paymentId = "pay-cur",
			amount = BigDecimal("10.00"),
			currency = "euro", // not a 3-letter ISO code
			occurredAt = Instant.parse("2026-06-02T10:00:00Z"),
		)

		assertThatThrownBy { processor.process(invalid) }
			.isInstanceOf(NonRetryableException::class.java)
			.hasMessageContaining("currency")

		assertThat(processedEventRepository.count()).isZero()
	}
}
