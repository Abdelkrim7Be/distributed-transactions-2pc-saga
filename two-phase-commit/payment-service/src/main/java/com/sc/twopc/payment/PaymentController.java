package com.sc.twopc.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/payment")
public class PaymentController {

	private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

	public static final String TRANSACTION_ID_HEADER = "X-Transaction-Id";

	private final PaymentService paymentService;

	public PaymentController(PaymentService paymentService) {
		this.paymentService = paymentService;
	}

	@PostMapping("/prepare")
	public ResponseEntity<?> prepare(
			@RequestHeader(value = TRANSACTION_ID_HEADER, required = false) String transactionId,
			@RequestParam(value = "fail", defaultValue = "false") boolean fail) {
		TwoPcStructuredLog.info(log, transactionId, "PREPARE received",
				"fail=" + fail + " | currentStatus=" + paymentService.getStatus());
		if (fail) {
			TwoPcStructuredLog.warn(log, transactionId, "PREPARE rejected",
					"simulated failure (fail=true) — returning HTTP 500");
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new ApiError("Simulated payment prepare failure"));
		}
		TransactionResponse body = paymentService.prepare(transactionId);
		TwoPcStructuredLog.info(log, transactionId, "PREPARE completed", "response status=" + body.status());
		return ResponseEntity.ok(body);
	}

	@PostMapping("/commit")
	public ResponseEntity<?> commit(
			@RequestHeader(value = TRANSACTION_ID_HEADER, required = false) String transactionId,
			@RequestParam(value = "fail", defaultValue = "false") boolean fail) {
		TwoPcStructuredLog.info(log, transactionId, "COMMIT received",
				"fail=" + fail + " | currentStatus=" + paymentService.getStatus());
		if (fail) {
			TwoPcStructuredLog.warn(log, transactionId, "COMMIT rejected",
					"simulated failure (fail=true) — returning HTTP 500");
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new ApiError("Simulated payment commit failure"));
		}
		TransactionResponse body = paymentService.commit(transactionId);
		TwoPcStructuredLog.info(log, transactionId, "COMMIT completed", "response status=" + body.status());
		return ResponseEntity.ok(body);
	}

	@PostMapping("/rollback")
	public ResponseEntity<TransactionResponse> rollback(
			@RequestHeader(value = TRANSACTION_ID_HEADER, required = false) String transactionId) {
		TwoPcStructuredLog.info(log, transactionId, "ROLLBACK received", "currentStatus=" + paymentService.getStatus());
		TransactionResponse body = paymentService.rollback(transactionId);
		TwoPcStructuredLog.info(log, transactionId, "ROLLBACK completed", "response status=" + body.status());
		return ResponseEntity.ok(body);
	}
}
