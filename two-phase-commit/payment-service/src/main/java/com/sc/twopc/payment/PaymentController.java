package com.sc.twopc.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/payment")
public class PaymentController {

	private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

	private final PaymentService paymentService;

	public PaymentController(PaymentService paymentService) {
		this.paymentService = paymentService;
	}

	@PostMapping("/prepare")
	public ResponseEntity<TransactionResponse> prepare() {
		log.info("POST /api/payment/prepare invoked | currentStatus={}", paymentService.getStatus());
		TransactionResponse body = paymentService.prepare();
		return ResponseEntity.ok(body);
	}

	@PostMapping("/commit")
	public ResponseEntity<TransactionResponse> commit() {
		log.info("POST /api/payment/commit invoked | currentStatus={}", paymentService.getStatus());
		TransactionResponse body = paymentService.commit();
		return ResponseEntity.ok(body);
	}

	@PostMapping("/rollback")
	public ResponseEntity<TransactionResponse> rollback() {
		log.info("POST /api/payment/rollback invoked | currentStatus={}", paymentService.getStatus());
		TransactionResponse body = paymentService.rollback();
		return ResponseEntity.ok(body);
	}
}
