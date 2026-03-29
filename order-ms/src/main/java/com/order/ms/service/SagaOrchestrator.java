package com.order.ms.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.order.ms.entity.Order;
import com.order.ms.entity.OrderRepository;
import com.order.ms.entity.OrderStatus;
import com.sc.saga.EventStatus;
import com.sc.saga.SagaEvent;
import com.sc.saga.SagaEventType;
import com.sc.saga.SagaTopics;

@Service
public class SagaOrchestrator {

	private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

	private final OrderRepository orderRepository;
	private final KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate;
	private final SagaOrchestrator self;

	public SagaOrchestrator(
			OrderRepository orderRepository,
			KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate,
			@Lazy SagaOrchestrator self) {
		this.orderRepository = orderRepository;
		this.sagaEventKafkaTemplate = sagaEventKafkaTemplate;
		this.self = self;
	}

	/**
	 * Orchestrates forward steps and compensation from stock, payment, and delivery topics.
	 */
	@KafkaListener(
			topics = { SagaTopics.STOCK_EVENTS, SagaTopics.PAYMENT_EVENTS, SagaTopics.DELIVERY_EVENTS },
			containerFactory = "sagaEventKafkaListenerContainerFactory",
			groupId = "order-saga-orchestrator")
	public void onParticipantSagaEvent(SagaEvent event) {
		self.handleParticipantEvent(event);
	}

	@Transactional
	public void handleParticipantEvent(SagaEvent event) {
		Long orderId = event.getOrderId();
		if (orderId == null) {
			return;
		}

		if (event.getStatus() == EventStatus.FAILED || isFailureEventType(event.getEventType())) {
			orderRepository.findById(orderId).ifPresent(this::compensate);
			return;
		}

		if (event.getStatus() != EventStatus.SUCCESS) {
			return;
		}

		switch (event.getEventType()) {
			case STOCK_RESERVED -> onStockReserved(orderId);
			case PAYMENT_PROCESSED -> onPaymentProcessed(orderId);
			case DELIVERY_SCHEDULED -> onDeliveryScheduled(orderId);
			default -> {
			}
		}
	}

	private static boolean isFailureEventType(SagaEventType type) {
		return type == SagaEventType.STOCK_RESERVE_FAILED
				|| type == SagaEventType.PAYMENT_FAILED
				|| type == SagaEventType.DELIVERY_FAILED;
	}

	@Transactional
	protected void onStockReserved(Long orderId) {
		orderRepository.findById(orderId).ifPresent(order -> {
			if (order.getStatus() != OrderStatus.CREATED) {
				return;
			}
			order.setStatus(OrderStatus.STOCK_RESERVED);
			orderRepository.save(order);
			SagaEvent next = SagaEvent.of(
					orderId,
					SagaEventType.PAYMENT_REQUESTED,
					EventStatus.SUCCESS,
					order.toSagaPayload());
			sagaEventKafkaTemplate.send(SagaTopics.PAYMENT_EVENTS, next);
			log.debug("orderId={} STOCK_RESERVED -> PAYMENT_REQUESTED", orderId);
		});
	}

	private void onPaymentProcessed(Long orderId) {
		orderRepository.findById(orderId).ifPresent(order -> {
			if (order.getStatus() != OrderStatus.STOCK_RESERVED) {
				return;
			}
			order.setStatus(OrderStatus.PAYMENT_PROCESSED);
			orderRepository.save(order);
			SagaEvent next = SagaEvent.of(
					orderId,
					SagaEventType.DELIVERY_REQUESTED,
					EventStatus.SUCCESS,
					order.toSagaPayload());
			sagaEventKafkaTemplate.send(SagaTopics.DELIVERY_EVENTS, next);
			log.debug("orderId={} PAYMENT_PROCESSED -> DELIVERY_REQUESTED", orderId);
		});
	}

	@Transactional
	protected void onDeliveryScheduled(Long orderId) {
		orderRepository.findById(orderId).ifPresent(order -> {
			if (order.getStatus() != OrderStatus.PAYMENT_PROCESSED) {
				return;
			}
			order.setStatus(OrderStatus.DELIVERED);
			orderRepository.save(order);
			log.debug("orderId={} DELIVERY_SCHEDULED -> DELIVERED", orderId);
		});
	}

	private void compensate(Order order) {
		OrderStatus previous = order.getStatus();
		if (previous == OrderStatus.COMPENSATING || previous == OrderStatus.FAILED) {
			return;
		}

		order.setStatus(OrderStatus.COMPENSATING);
		orderRepository.save(order);

		var payload = order.toSagaPayload();

		if (previous == OrderStatus.DELIVERED) {
			send(SagaTopics.DELIVERY_EVENTS, SagaEventType.DELIVERY_CANCELLED, order.getId(), payload);
		}
		if (previous == OrderStatus.DELIVERED || previous == OrderStatus.PAYMENT_PROCESSED) {
			send(SagaTopics.PAYMENT_EVENTS, SagaEventType.PAYMENT_REFUNDED, order.getId(), payload);
		}
		if (previous == OrderStatus.DELIVERED
				|| previous == OrderStatus.PAYMENT_PROCESSED
				|| previous == OrderStatus.STOCK_RESERVED) {
			send(SagaTopics.STOCK_EVENTS, SagaEventType.STOCK_RELEASED, order.getId(), payload);
		}

		sagaEventKafkaTemplate.send(
				SagaTopics.ORDER_EVENTS,
				SagaEvent.of(order.getId(), SagaEventType.ORDER_FAILED, EventStatus.FAILED, payload));

		order.setStatus(OrderStatus.FAILED);
		orderRepository.save(order);
		log.warn("orderId={} compensation finished from state {}", order.getId(), previous);
	}

	private void send(String topic, SagaEventType type, Long orderId, java.util.Map<String, Object> payload) {
		sagaEventKafkaTemplate.send(topic, SagaEvent.of(orderId, type, EventStatus.SUCCESS, payload));
	}
}
