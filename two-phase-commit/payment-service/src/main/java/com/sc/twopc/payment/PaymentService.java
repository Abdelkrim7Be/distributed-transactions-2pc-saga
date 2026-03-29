package com.sc.twopc.payment;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {

	private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

	public static final String INITIAL = "INITIAL";
	public static final String PREPARED = "PREPARED";
	public static final String COMMITTED = "COMMITTED";
	public static final String ROLLED_BACK = "ROLLED_BACK";

	private final Object lock = new Object();
	private String status = INITIAL;

	public String getStatus() {
		synchronized (lock) {
			return status;
		}
	}

	public TransactionResponse prepare() {
		synchronized (lock) {
			String previous = status;
			status = PREPARED;
			log.info("{} | payment prepare | {} -> {}", Instant.now(), previous, status);
			return new TransactionResponse(status, "Payment prepared (funds checked)");
		}
	}

	public TransactionResponse commit() {
		synchronized (lock) {
			String previous = status;
			status = COMMITTED;
			log.info("{} | payment commit | {} -> {}", Instant.now(), previous, status);
			return new TransactionResponse(status, "Payment committed");
		}
	}

	public TransactionResponse rollback() {
		synchronized (lock) {
			String previous = status;
			status = ROLLED_BACK;
			log.info("{} | payment rollback | {} -> {}", Instant.now(), previous, status);
			return new TransactionResponse(status, "Payment rolled back");
		}
	}
}
