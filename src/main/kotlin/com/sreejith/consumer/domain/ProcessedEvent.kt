package com.sreejith.consumer.domain

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

/** One row per handled event id; the primary key is the dedupe guarantee. */
@Entity
@Table(name = "processed_event")
class ProcessedEvent(
	@Id
	@Column(name = "event_id", length = 36)
	val eventId: String,

	@Column(name = "payment_id", length = 36, nullable = false)
	val paymentId: String,

	@Column(name = "processed_at", nullable = false)
	val processedAt: Instant,
)
