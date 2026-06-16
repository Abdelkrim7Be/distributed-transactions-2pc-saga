package com.payment.ms.controller;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.payment.ms.entity.Payment;
import com.payment.ms.entity.PaymentRepository;

@RestController
@RequestMapping("/api/2pc/payment")
public class Payment2PCController {

	private static final Logger log = LoggerFactory.getLogger(Payment2PCController.class);

	private final PaymentRepository paymentRepository;
	private final Map<Long, Double> pendingPayments = new ConcurrentHashMap<>();

	public Payment2PCController(PaymentRepository paymentRepository) {
		this.paymentRepository = paymentRepository;
	}

	@PostMapping("/prepare")
	public ResponseEntity<String> prepare(@RequestBody Map<String, Object> request) {
		Long orderId = Long.valueOf(request.get("orderId").toString());
		Double amount = Double.valueOf(request.get("amount").toString());

		log.info("2PC PREPARE | orderId={} amount={}", orderId, amount);

		if (amount > 10000) {
			log.warn("2PC PREPARE REJECTED | orderId={} reason=limit_exceeded", orderId);
			return ResponseEntity.status(409).body("Amount limit exceeded");
		}

		pendingPayments.put(orderId, amount);
		return ResponseEntity.ok("PREPARED");
	}

	@PostMapping("/commit")
	public ResponseEntity<String> commit(@RequestBody Map<String, Object> request) {
		Long orderId = Long.valueOf(request.get("orderId").toString());
		Double amount = pendingPayments.remove(orderId);

		log.info("2PC COMMIT | orderId={}", orderId);

		if (amount != null) {
			Payment payment = new Payment();
			payment.setOrderId(orderId);
			payment.setAmount(amount);
			payment.setStatus("SUCCESS");
			payment.setMode("2PC");
			paymentRepository.save(payment);
		}

		return ResponseEntity.ok("COMMITTED");
	}

	@PostMapping("/rollback")
	public ResponseEntity<String> rollback(@RequestBody Map<String, Object> request) {
		Long orderId = Long.valueOf(request.get("orderId").toString());
		log.info("2PC ROLLBACK | orderId={}", orderId);
		pendingPayments.remove(orderId);
		return ResponseEntity.ok("ROLLED_BACK");
	}
}
