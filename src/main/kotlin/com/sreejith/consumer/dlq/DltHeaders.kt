package com.sreejith.consumer.dlq

/**
 * App-owned headers attached to every dead-lettered record, on top of Spring's
 * standard `kafka_dlt-*` headers. These give the DLQ inspector and replay path
 * (Phase 4) a stable, framework-independent contract for debugging context.
 *
 * The original payload is carried as the DLT record's *value* (not a header).
 */
object DltHeaders {
	/** Cleaned-up exception message describing why the record failed. */
	const val EXCEPTION_MESSAGE = "x-dlt-exception-message"
	/** Total delivery attempts made before the record was dead-lettered. */
	const val ATTEMPTS = "x-dlt-attempts"
	/** Wall-clock instant (ISO-8601) at which the record was dead-lettered. */
	const val FAILED_AT = "x-dlt-failed-at"
	/** The main topic the record originally came from — the replay target. */
	const val ORIGINAL_TOPIC = "x-dlt-original-topic"
}
