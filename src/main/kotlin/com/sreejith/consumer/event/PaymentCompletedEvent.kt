package com.sreejith.consumer.event

import java.math.BigDecimal
import java.time.Instant

/**
 * Consumer-side copy of the contract published to `payment-events`. The two
 * services are separate repos with no shared module, so the schema is duplicated
 * deliberately; unknown fields are ignored on parse to tolerate producer additions.
 */
data class PaymentCompletedEvent(
	val eventId: String,
	val paymentId: String,
	val amount: BigDecimal,
	val currency: String,
	val occurredAt: Instant,
)
