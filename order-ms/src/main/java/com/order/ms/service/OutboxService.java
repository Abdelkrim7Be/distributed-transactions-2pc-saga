package com.order.ms.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.ms.entity.OutboxEvent;
import com.order.ms.entity.OutboxRepository;
import com.sc.saga.SagaEvent;

@Service
public class OutboxService {

	private static final Logger log = LoggerFactory.getLogger(OutboxService.class);

	private final OutboxRepository outboxRepository;
	private final ObjectMapper objectMapper;

	public OutboxService(OutboxRepository outboxRepository, ObjectMapper objectMapper) {
		this.outboxRepository = outboxRepository;
		this.objectMapper = objectMapper;
	}

	@Transactional(propagation = Propagation.MANDATORY)
	public void saveEvent(String topic, SagaEvent event) {
		try {
			String payload = objectMapper.writeValueAsString(event);
			OutboxEvent outboxEvent = new OutboxEvent();
			outboxEvent.setTopic(topic);
			outboxEvent.setPayload(payload);
			outboxRepository.save(outboxEvent);
			log.debug("Saved event {} to outbox for topic {}", event.getEventId(), topic);
		} catch (JsonProcessingException e) {
			log.error("Failed to serialize SagaEvent", e);
			throw new RuntimeException("Serialization error", e);
		}
	}
}
