package com.sreejith.consumer.repository

import com.sreejith.consumer.domain.ProcessedEvent
import org.springframework.data.jpa.repository.JpaRepository

interface ProcessedEventRepository : JpaRepository<ProcessedEvent, String>
