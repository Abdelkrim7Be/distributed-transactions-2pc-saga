package com.order.ms.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.order.ms.dto.CreateOrderRequest;
import com.order.ms.dto.OrderResponse;
import com.order.ms.entity.Order;
import com.order.ms.service.OrderService;

@RestController
@RequestMapping("/api")
public class OrderController {

	private final OrderService orderService;

	public OrderController(OrderService orderService) {
		this.orderService = orderService;
	}

	@GetMapping("/order/{id}")
	public ResponseEntity<Order> getOrderById(@PathVariable Long id) {
		return orderService.getOrderById(id).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
	}

	@PostMapping("/order")
	public ResponseEntity<OrderResponse> createOrder(@RequestBody CreateOrderRequest request) {
		return ResponseEntity.ok(orderService.startSaga(request));
	}
}
