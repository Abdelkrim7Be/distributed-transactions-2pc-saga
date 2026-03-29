package com.sc.saga;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Canonical Kafka message for the saga pipeline. Compatible with {@link org.springframework.kafka.support.serializer.JsonSerializer}
 * / {@link org.springframework.kafka.support.serializer.JsonDeserializer}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SagaEvent {

	private UUID eventId;
	private Long orderId;
	private SagaEventType eventType;
	private EventStatus status;
	private Instant timestamp;
	private Map<String, Object> payload;

	public SagaEvent() {
		this.payload = new HashMap<>();
	}

	public static SagaEvent of(Long orderId, SagaEventType eventType, EventStatus status, Map<String, ?> payload) {
		SagaEvent e = new SagaEvent();
		e.setEventId(UUID.randomUUID());
		e.setOrderId(orderId);
		e.setEventType(eventType);
		e.setStatus(status);
		e.setTimestamp(Instant.now());
		if (payload != null && !payload.isEmpty()) {
			e.getPayload().putAll(payload);
		}
		return e;
	}

	public UUID getEventId() {
		return eventId;
	}

	public void setEventId(UUID eventId) {
		this.eventId = eventId;
	}

	public Long getOrderId() {
		return orderId;
	}

	public void setOrderId(Long orderId) {
		this.orderId = orderId;
	}

	public SagaEventType getEventType() {
		return eventType;
	}

	public void setEventType(SagaEventType eventType) {
		this.eventType = eventType;
	}

	public EventStatus getStatus() {
		return status;
	}

	public void setStatus(EventStatus status) {
		this.status = status;
	}

	public Instant getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Instant timestamp) {
		this.timestamp = timestamp;
	}

	public Map<String, Object> getPayload() {
		if (payload == null) {
			payload = new HashMap<>();
		}
		return payload;
	}

	public void setPayload(Map<String, Object> payload) {
		this.payload = payload != null ? new HashMap<>(payload) : new HashMap<>();
	}
}
