package com.sreejith.consumer.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Explicit ObjectMapper — Spring Boot only auto-configures one when spring-web
 * is on the classpath, and this service has no web layer. Configured to match
 * the producer's wire format: ISO-8601 timestamps, tolerant of unknown fields.
 */
@Configuration
class JacksonConfig {
	@Bean
	fun objectMapper(): ObjectMapper =
		jacksonObjectMapper().apply {
			registerModule(JavaTimeModule())
			disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
			configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
		}
}
