package com.sreejith.consumer.repository

import com.sreejith.consumer.domain.PaymentAudit
import org.springframework.data.jpa.repository.JpaRepository

interface PaymentAuditRepository : JpaRepository<PaymentAudit, Long> {
	fun countByPaymentId(paymentId: String): Long
}
