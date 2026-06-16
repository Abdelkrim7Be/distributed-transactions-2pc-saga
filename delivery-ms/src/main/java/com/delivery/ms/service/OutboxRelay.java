package com.delivery.ms.service;

import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.delivery.ms.entity.OutboxEvent;
import com.delivery.ms.entity.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sc.saga.SagaEvent;

@Service
public class OutboxRelay {

	private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

	private final OutboxRepository outboxRepository;
	private final KafkaTemplate<String, SagaEvent> kafkaTemplate;
	private final ObjectMapper objectMapper;

	public OutboxRelay(OutboxRepository outboxRepository, KafkaTemplate<String, SagaEvent> kafkaTemplate, ObjectMapper objectMapper) {
		this.outboxRepository = outboxRepository;
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}

	@Scheduled(fixedDelay = 5000)
	@Transactional
	public void relayEvents() {
		List<OutboxEvent> pendingEvents = outboxRepository.findByStatusOrderByCreatedAtAsc("PENDING");
		if (pendingEvents.isEmpty()) {
			return;
		}

		log.debug("Found {} pending outbox events", pendingEvents.size());
		for (OutboxEvent event : pendingEvents) {
			try {
				SagaEvent sagaEvent = objectMapper.readValue(event.getPayload(), SagaEvent.class);
				kafkaTemplate.send(event.getTopic(), sagaEvent).whenComplete((result, ex) -> {
					if (ex == null) {
						log.debug("Successfully relayed outbox event {}", event.getId());
					} else {
						log.error("Failed to relay outbox event {}", event.getId(), ex);
					}
				});
				event.setStatus("PROCESSED");
				event.setProcessedAt(Instant.now());
				outboxRepository.save(event);
			} catch (JsonProcessingException e) {
				log.error("Failed to deserialize OutboxEvent payload", e);
				event.setStatus("FAILED");
				outboxRepository.save(event);
			}
		}
	}
}
