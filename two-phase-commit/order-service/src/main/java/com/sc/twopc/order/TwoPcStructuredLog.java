package com.sc.twopc.order;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;

final class TwoPcStructuredLog {

	static final String SERVICE_NAME = "ORDER-SERVICE";

	private static final DateTimeFormatter TIMESTAMP =
			DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

	private TwoPcStructuredLog() {
	}

	static String txOrDash(String transactionId) {
		if (transactionId == null || transactionId.isBlank()) {
			return "—";
		}
		return transactionId;
	}

	static void info(Logger logger, String transactionId, String action, String details) {
		logger.info("[{}] [{}] [{}] {} — {}", TIMESTAMP.format(Instant.now()), SERVICE_NAME, txOrDash(transactionId), action,
				details);
	}

	static void warn(Logger logger, String transactionId, String action, String details) {
		logger.warn("[{}] [{}] [{}] {} — {}", TIMESTAMP.format(Instant.now()), SERVICE_NAME, txOrDash(transactionId), action,
				details);
	}
}
