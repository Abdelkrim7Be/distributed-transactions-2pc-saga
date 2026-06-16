package com.payment.ms.entity;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

	@Id
	@GeneratedValue(strategy = jakarta.persistence.GenerationType.UUID)
	private UUID id;

	@Column(nullable = false)
	private String topic;

	@Lob
	@Column(nullable = false)
	private String payload;

	@Column(nullable = false)
	private String status = "PENDING";

	@Column(nullable = false)
	private Instant createdAt = Instant.now();

	@Column
	private Instant processedAt;
}
