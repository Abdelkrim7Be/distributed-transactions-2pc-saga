package com.sc.twopc.payment;

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

	public TransactionResponse prepare(String transactionId) {
		synchronized (lock) {
			String previous = status;
			status = PREPARED;
			TwoPcStructuredLog.info(log, transactionId, "PREPARE applied",
					"status changed from " + previous + " to " + status);
			return new TransactionResponse(status, "Payment prepared (funds checked)");
		}
	}

	public TransactionResponse commit(String transactionId) {
		synchronized (lock) {
			String previous = status;
			status = COMMITTED;
			TwoPcStructuredLog.info(log, transactionId, "COMMIT applied",
					"status changed from " + previous + " to " + status);
			return new TransactionResponse(status, "Payment committed");
		}
	}

	public TransactionResponse rollback(String transactionId) {
		synchronized (lock) {
			String previous = status;
			status = ROLLED_BACK;
			TwoPcStructuredLog.info(log, transactionId, "ROLLBACK applied",
					"status changed from " + previous + " to " + status);
			return new TransactionResponse(status, "Payment rolled back");
		}
	}
}
