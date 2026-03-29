package com.sc.twopc.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/transaction")
public class CoordinatorController {

	private static final Logger log = LoggerFactory.getLogger(CoordinatorController.class);

	private final CoordinatorService coordinatorService;

	public CoordinatorController(CoordinatorService coordinatorService) {
		this.coordinatorService = coordinatorService;
	}

	@PostMapping("/start")
	public ResponseEntity<StartTransactionResponse> start(
			@RequestBody(required = false) StartTransactionRequest request) {
		TwoPcStructuredLog.info(log, null, "POST /api/transaction/start received", "orchestration request accepted");
		if (request == null) {
			request = new StartTransactionRequest();
		}
		StartTransactionResponse body = coordinatorService.startTransaction(request);
		TwoPcStructuredLog.info(
				log,
				body.transactionId(),
				"POST /api/transaction/start completed",
				"overallResult=" + body.overallResult() + " | order=" + body.orderStatus() + " | payment="
						+ body.paymentStatus());
		return ResponseEntity.ok(body);
	}
}
