package com.sreejith.consumer.dlq

import java.time.Instant

/**
 * A dead-lettered message as seen by the inspector: the original payload plus the
 * failure context decoded from [DltHeaders]. `partition`/`offset` locate it on the
 * dead-letter topic; `key` and `payload` are what get republished on replay.
 */
data class DeadLetterRecord(
	val key: String?,
	val payload: String,
	val originalTopic: String,
	val exceptionMessage: String?,
	val attempts: Int?,
	val failedAt: Instant?,
	val partition: Int,
	val offset: Long,
)
