package com.sreejith.consumer.support

import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/** Shared MySQL container; Flyway migrates it on context startup. */
@Testcontainers
abstract class AbstractMySqlIntegrationTest {

	companion object {
		@Container
		@JvmStatic
		val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4")
			.withDatabaseName("payments_consumer")
			.withUsername("consumer")
			.withPassword("consumer")

		@DynamicPropertySource
		@JvmStatic
		fun datasourceProps(registry: DynamicPropertyRegistry) {
			registry.add("spring.datasource.url", mysql::getJdbcUrl)
			registry.add("spring.datasource.username", mysql::getUsername)
			registry.add("spring.datasource.password", mysql::getPassword)
		}
	}
}
