package com.sc.twopc.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/order")
public class OrderController {

	private static final Logger log = LoggerFactory.getLogger(OrderController.class);

	private final OrderService orderService;

	public OrderController(OrderService orderService) {
		this.orderService = orderService;
	}

	@PostMapping("/prepare")
	public ResponseEntity<TransactionResponse> prepare() {
		log.info("POST /api/order/prepare invoked | currentStatus={}", orderService.getStatus());
		TransactionResponse body = orderService.prepare();
		return ResponseEntity.ok(body);
	}

	@PostMapping("/commit")
	public ResponseEntity<TransactionResponse> commit() {
		log.info("POST /api/order/commit invoked | currentStatus={}", orderService.getStatus());
		TransactionResponse body = orderService.commit();
		return ResponseEntity.ok(body);
	}

	@PostMapping("/rollback")
	public ResponseEntity<TransactionResponse> rollback() {
		log.info("POST /api/order/rollback invoked | currentStatus={}", orderService.getStatus());
		TransactionResponse body = orderService.rollback();
		return ResponseEntity.ok(body);
	}
}
