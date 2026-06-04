package com.sreejith.consumer.error

/**
 * Marks a failure that cannot succeed on retry: a malformed payload or a broken
 * business rule (validation error). Throwing this is the signal that the message
 * should SKIP retries and go straight to the dead-letter topic — wired into the
 * retry machinery in Phases 2–3 via `@RetryableTopic(exclude = ...)`.
 *
 * Any failure NOT represented by this type is treated as transient (e.g. a broker
 * or database hiccup) and is retried with backoff.
 */
class NonRetryableException(message: String, cause: Throwable? = null) :
	RuntimeException(message, cause)
