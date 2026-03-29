package com.sc.twopc.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
	public ResponseEntity<?> prepare(@RequestParam(value = "fail", defaultValue = "false") boolean fail) {
		log.info("POST /api/payment/prepare invoked | fail={} | currentStatus={}", fail, paymentService.getStatus());
		if (fail) {
			log.warn("Simulated payment prepare failure (fail=true)");
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new ApiError("Simulated payment prepare failure"));
		}
		TransactionResponse body = paymentService.prepare();
		return ResponseEntity.ok(body);
	}

	@PostMapping("/commit")
	public ResponseEntity<?> commit(@RequestParam(value = "fail", defaultValue = "false") boolean fail) {
		log.info("POST /api/payment/commit invoked | fail={} | currentStatus={}", fail, paymentService.getStatus());
		if (fail) {
			log.warn("Simulated payment commit failure (fail=true)");
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new ApiError("Simulated payment commit failure"));
		}
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
