package com.sreejith.consumer.support

import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.MySQLContainer

/**
 * Base class for tests needing a real MySQL.
 *
 * Uses the Testcontainers *singleton container* pattern: started once in a
 * static initializer and never explicitly stopped, so it is reused across every
 * test class and survives Spring's context cache (Ryuk reaps it at JVM exit).
 */
abstract class AbstractMySqlIntegrationTest {

	companion object {
		@JvmStatic
		val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4")
			.withDatabaseName("payments_consumer")
			.withUsername("consumer")
			.withPassword("consumer")
			.also { it.start() }

		@DynamicPropertySource
		@JvmStatic
		fun datasourceProps(registry: DynamicPropertyRegistry) {
			registry.add("spring.datasource.url", mysql::getJdbcUrl)
			registry.add("spring.datasource.username", mysql::getUsername)
			registry.add("spring.datasource.password", mysql::getPassword)
		}
	}
}
