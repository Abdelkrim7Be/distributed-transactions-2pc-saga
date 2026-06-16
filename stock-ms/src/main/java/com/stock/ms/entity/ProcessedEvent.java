package com.stock.ms.entity;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "inbox_events")
public class ProcessedEvent {

	@Id
	private UUID eventId;

	@Column(nullable = false)
	private Instant processedAt = Instant.now();
}
