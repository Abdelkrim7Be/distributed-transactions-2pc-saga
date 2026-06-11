package com.order.ms.service;

import java.time.Instant;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.order.ms.entity.CompensationStatus;
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
			log.debug("{} | orderId={} action=COMPENSATION_SKIP already terminal status={}", Instant.now(), order.getId(), previous);
			return;
		}

		Long orderId = order.getId();
		boolean needDeliveryCancel = previous == OrderStatus.DELIVERED;
		boolean needRefund = previous == OrderStatus.DELIVERED || previous == OrderStatus.PAYMENT_PROCESSED;
		boolean needStockRelease = previous == OrderStatus.DELIVERED
				|| previous == OrderStatus.PAYMENT_PROCESSED
				|| previous == OrderStatus.STOCK_RESERVED;

		order.setStatus(OrderStatus.COMPENSATING);
		if (needDeliveryCancel || needRefund || needStockRelease) {
			order.setCompensationStatus(CompensationStatus.IN_PROGRESS);
		}
		orderRepository.save(order);
		log.info(
				"{} | orderId={} action=COMPENSATION_START previousStatus={} compensationStatus={} steps=[deliveryCancel={},refund={},stockRelease={}]",
				Instant.now(),
				orderId,
				previous,
				order.getCompensationStatus(),
				needDeliveryCancel,
				needRefund,
				needStockRelease);

		Map<String, Object> payload = order.toSagaPayload();

		if (needDeliveryCancel) {
			publishCompensation(SagaTopics.DELIVERY_EVENTS, SagaEventType.DELIVERY_CANCELLED, orderId, payload, "DELIVERY_CANCELLED");
			order.setCompensationStatus(CompensationStatus.DELIVERY_CANCEL_PUBLISHED);
			orderRepository.save(order);
			log.info(
					"{} | orderId={} action=COMPENSATION_STEP_DONE step=DELIVERY_CANCELLED compensationStatus={}",
					Instant.now(),
					orderId,
					order.getCompensationStatus());
		}

		if (needRefund) {
			publishCompensation(SagaTopics.PAYMENT_EVENTS, SagaEventType.PAYMENT_REFUNDED, orderId, payload, "PAYMENT_REFUNDED");
			order.setCompensationStatus(CompensationStatus.PAYMENT_REFUND_PUBLISHED);
			orderRepository.save(order);
			log.info(
					"{} | orderId={} action=COMPENSATION_STEP_DONE step=PAYMENT_REFUNDED compensationStatus={}",
					Instant.now(),
					orderId,
					order.getCompensationStatus());
		}

		if (needStockRelease) {
			publishCompensation(SagaTopics.STOCK_EVENTS, SagaEventType.STOCK_RELEASED, orderId, payload, "STOCK_RELEASED");
			order.setCompensationStatus(CompensationStatus.STOCK_RELEASE_PUBLISHED);
			orderRepository.save(order);
			log.info(
					"{} | orderId={} action=COMPENSATION_STEP_DONE step=STOCK_RELEASED compensationStatus={}",
					Instant.now(),
					orderId,
					order.getCompensationStatus());
		}

		if (!needDeliveryCancel && !needRefund && !needStockRelease) {
			log.info(
					"{} | orderId={} action=COMPENSATION_SKIP_STEPS reason=no_completed_saga_steps (e.g. stock failed before reserve)",
					Instant.now(),
					orderId);
		}

		sagaEventKafkaTemplate.send(
				SagaTopics.ORDER_EVENTS,
				SagaEvent.of(orderId, SagaEventType.ORDER_FAILED, EventStatus.FAILED, payload));
		log.info(
				"{} | orderId={} action=COMPENSATION_PUBLISH step=ORDER_FAILED topic={}",
				Instant.now(),
				orderId,
				SagaTopics.ORDER_EVENTS);

		order.setStatus(OrderStatus.FAILED);
		order.setCompensationStatus(CompensationStatus.COMPLETED);
		orderRepository.save(order);
		log.warn(
				"{} | orderId={} action=COMPENSATION_COMPLETE previousStatus={} finalCompensationStatus={}",
				Instant.now(),
				orderId,
				previous,
				CompensationStatus.COMPLETED);
	}

	private void publishCompensation(String topic, SagaEventType type, Long orderId, Map<String, Object> payload, String stepLabel) {
		log.info(
				"{} | orderId={} action=COMPENSATION_PUBLISH step={} topic={} eventType={}",
				Instant.now(),
				orderId,
				stepLabel,
				topic,
				type);
		sagaEventKafkaTemplate.send(topic, SagaEvent.of(orderId, type, EventStatus.SUCCESS, payload));
	}
}
