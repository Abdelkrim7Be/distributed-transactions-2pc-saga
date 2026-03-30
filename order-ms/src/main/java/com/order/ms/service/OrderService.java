package com.order.ms.service;

import java.util.Optional;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.order.ms.dto.CreateOrderRequest;
import com.order.ms.dto.OrderResponse;
import com.order.ms.entity.CompensationStatus;
import com.order.ms.entity.Order;
import com.order.ms.entity.OrderRepository;
import com.order.ms.entity.OrderStatus;
import com.sc.saga.EventStatus;
import com.sc.saga.SagaEvent;
import com.sc.saga.SagaEventType;
import com.sc.saga.SagaTopics;

@Service
public class OrderService {

	private final OrderRepository orderRepository;
	private final KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate;

	public OrderService(OrderRepository orderRepository, KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate) {
		this.orderRepository = orderRepository;
		this.sagaEventKafkaTemplate = sagaEventKafkaTemplate;
	}

	@Transactional(readOnly = true)
	public Optional<Order> getOrderById(Long id) {
		return orderRepository.findById(id);
	}

	@Transactional
	public OrderResponse startSaga(CreateOrderRequest request) {
		if (request.getProductId() == null) {
			throw new IllegalArgumentException("productId is required");
		}

		Order order = new Order();
		order.setProductId(request.getProductId());
		order.setQuantity(request.getQuantity());
		order.setAmount(request.getAmount());
		order.setStatus(OrderStatus.CREATED);
		order.setCompensationStatus(CompensationStatus.NONE);
		if (request.getFailAt() != null && !request.getFailAt().isBlank()) {
			order.setFailAt(request.getFailAt().trim());
		}
		order = orderRepository.save(order);

		SagaEvent created = SagaEvent.of(
				order.getId(),
				SagaEventType.ORDER_CREATED,
				EventStatus.SUCCESS,
				order.toSagaPayload());
		sagaEventKafkaTemplate.send(SagaTopics.ORDER_EVENTS, created);

		return new OrderResponse(order.getId(), order.getStatus());
	}
}
