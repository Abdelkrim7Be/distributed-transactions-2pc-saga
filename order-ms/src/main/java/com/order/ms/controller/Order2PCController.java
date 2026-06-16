package com.order.ms.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.order.ms.dto.CreateOrderRequest;
import com.order.ms.entity.CompensationStatus;
import com.order.ms.entity.Order;
import com.order.ms.entity.OrderRepository;
import com.order.ms.entity.OrderStatus;
import com.order.ms.service.TwoPhaseCommitCoordinator;

@RestController
@RequestMapping("/api/2pc/order")
public class Order2PCController {

	private final OrderRepository orderRepository;
	private final TwoPhaseCommitCoordinator coordinator;

	public Order2PCController(OrderRepository orderRepository, TwoPhaseCommitCoordinator coordinator) {
		this.orderRepository = orderRepository;
		this.coordinator = coordinator;
	}

	@PostMapping
	public ResponseEntity<?> createOrder2PC(@RequestBody CreateOrderRequest request) {
		// Save initial order
		Order order = new Order();
		order.setProductId(request.getProductId());
		order.setQuantity(request.getQuantity());
		order.setAmount(request.getAmount());
		order.setStatus(OrderStatus.CREATED);
		order.setCompensationStatus(CompensationStatus.NONE);
		order = orderRepository.save(order);

		// Synchronous 2PC coordination
		boolean success = coordinator.coordinate(
				order.getId(),
				order.getProductId(),
				order.getQuantity(),
				order.getAmount());

		if (success) {
			order.setStatus(OrderStatus.DELIVERED);
			orderRepository.save(order);
			return ResponseEntity.ok(Map.of("id", order.getId(), "status", "SUCCESS (2PC)", "message", "Strong consistency achieved via synchronous locks"));
		} else {
			order.setStatus(OrderStatus.FAILED);
			orderRepository.save(order);
			return ResponseEntity.status(409).body(Map.of("id", order.getId(), "status", "FAILED (2PC)", "message", "Transaction rolled back during Prepare phase"));
		}
	}
}
