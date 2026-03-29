package com.sc.twopc.coordinator;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.slf4j.Logger;

final class TwoPcStructuredLog {

	static final String SERVICE_NAME = "COORDINATOR-SERVICE";

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

	static void logTransactionSummary(
			Logger logger,
			String transactionId,
			List<String> timeline,
			String overallResult,
			String orderStatus,
			String paymentStatus) {
		String ts = TIMESTAMP.format(Instant.now());
		String tx = txOrDash(transactionId);
		StringBuilder sb = new StringBuilder(256);
		sb.append('\n');
		sb.append(String.format(
				"[%s] [%s] [%s] TRANSACTION SUMMARY — overallResult=%s | orderStatus=%s | paymentStatus=%s",
				ts, SERVICE_NAME, tx, overallResult, orderStatus, paymentStatus));
		sb.append('\n');
		sb.append(String.format("[%s] [%s] [%s] TIMELINE —", TIMESTAMP.format(Instant.now()), SERVICE_NAME, tx));
		for (int i = 0; i < timeline.size(); i++) {
			sb.append(String.format("%n  %d. %s", i + 1, timeline.get(i)));
		}
		logger.info(sb.toString());
	}
}
