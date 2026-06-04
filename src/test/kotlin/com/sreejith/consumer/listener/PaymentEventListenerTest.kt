package com.sreejith.consumer.listener

import com.sreejith.consumer.config.JacksonConfig
import com.sreejith.consumer.error.NonRetryableException
import com.sreejith.consumer.event.PaymentCompletedEvent
import com.sreejith.consumer.service.IdempotentPaymentProcessor
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Plain unit test (no Spring, no broker) of the listener's failure classification:
 * an unparseable payload is surfaced as [NonRetryableException]; a well-formed one
 * is handed to the processor untouched.
 */
class PaymentEventListenerTest {

	private val processor = mockk<IdempotentPaymentProcessor>()
	private val objectMapper = JacksonConfig().objectMapper()
	private val listener = PaymentEventListener(processor, objectMapper)

	@Test
	fun `a malformed payload is classified as non-retryable`() {
		val record = record("{ this is not json")

		assertThatThrownBy { listener.onMessage(record) }
			.isInstanceOf(NonRetryableException::class.java)
			.hasMessageContaining("Malformed payment event payload")

		verify(exactly = 0) { processor.process(any()) }
	}

	@Test
	fun `a well-formed payload is delegated to the processor`() {
		every { processor.process(any()) } returns true
		val json = """
			{"eventId":"evt-1","paymentId":"pay-1","amount":12.50,
			 "currency":"EUR","occurredAt":"2026-06-02T10:00:00Z"}
		""".trimIndent()

		listener.onMessage(record(json))

		verify(exactly = 1) {
			processor.process(
				match<PaymentCompletedEvent> { it.eventId == "evt-1" && it.paymentId == "pay-1" },
			)
		}
	}

	private fun record(value: String) =
		ConsumerRecord("payment-events", 0, 0L, "pay-1", value)
}
