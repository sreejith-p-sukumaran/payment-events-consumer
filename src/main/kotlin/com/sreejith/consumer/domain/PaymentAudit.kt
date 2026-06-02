package com.sreejith.consumer.domain

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.math.BigDecimal
import java.time.Instant

/** The observable effect of processing a payment event — one per applied event. */
@Entity
@Table(name = "payment_audit")
class PaymentAudit(
	@Column(name = "payment_id", length = 36, nullable = false)
	val paymentId: String,

	@Column(name = "amount", nullable = false)
	val amount: BigDecimal,

	@Column(name = "currency", length = 3, nullable = false)
	val currency: String,

	@Column(name = "recorded_at", nullable = false)
	val recordedAt: Instant,

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "id")
	val id: Long? = null,
)
