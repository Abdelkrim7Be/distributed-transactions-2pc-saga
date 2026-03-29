package com.sc.twopc.coordinator;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
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
	public ResponseEntity<StartTransactionResponse> start() {
		log.info("{} | POST /api/transaction/start invoked", Instant.now());
		StartTransactionResponse body = coordinatorService.startTransaction();
		return ResponseEntity.ok(body);
	}
}
