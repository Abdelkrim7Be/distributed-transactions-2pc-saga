package com.sc.twopc.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Service
public class CoordinatorService {

	private static final Logger log = LoggerFactory.getLogger(CoordinatorService.class);

	private static final String TRANSACTION_ID_HEADER = "X-Transaction-Id";

	private final RestTemplate restTemplate;
	private final String orderServiceUrl;
	private final String paymentServiceUrl;
	private final ConcurrentMap<String, String> transactionStates = new ConcurrentHashMap<>();

	public CoordinatorService(
			RestTemplate restTemplate,
			@Value("${coordinator.services.order}") String orderServiceUrl,
			@Value("${coordinator.services.payment}") String paymentServiceUrl) {
		this.restTemplate = restTemplate;
		this.orderServiceUrl = orderServiceUrl;
		this.paymentServiceUrl = paymentServiceUrl;
	}

	public StartTransactionResponse startTransaction(StartTransactionRequest flags) {
		if (flags == null) {
			flags = new StartTransactionRequest();
		}

		String transactionId = UUID.randomUUID().toString();
		long t0 = System.nanoTime();
		List<String> timeline = new ArrayList<>();

		TwoPcStructuredLog.info(log, transactionId, "TRANSACTION started", "assigned transactionId; state=PREPARING");
		transactionStates.put(transactionId, "PREPARING");
		addElapsed(timeline, t0, "Transaction created; coordinator state PREPARING");

		String orderPrepareUrl = withFailParam(orderServiceUrl + "/prepare", flags.isOrderPrepareFail());
		TwoPcStructuredLog.info(log, transactionId, "Phase 1 outbound", "POST order prepare -> " + orderPrepareUrl);
		addElapsed(timeline, t0, "Phase 1: sending Order PREPARE → " + orderPrepareUrl);
		PrepareOutcome orderPrepare = postPrepare(orderPrepareUrl, transactionId);
		addElapsed(
				timeline,
				t0,
				orderPrepare.success()
						? "Phase 1: Order PREPARE → HTTP 2xx (success)"
						: "Phase 1: Order PREPARE → failed (non-2xx or client error)");

		String paymentPrepareUrl = withFailParam(paymentServiceUrl + "/prepare", flags.isPaymentPrepareFail());
		TwoPcStructuredLog.info(log, transactionId, "Phase 1 outbound", "POST payment prepare -> " + paymentPrepareUrl);
		addElapsed(timeline, t0, "Phase 1: sending Payment PREPARE → " + paymentPrepareUrl);
		PrepareOutcome paymentPrepare = postPrepare(paymentPrepareUrl, transactionId);
		addElapsed(
				timeline,
				t0,
				paymentPrepare.success()
						? "Phase 1: Payment PREPARE → HTTP 2xx (success)"
						: "Phase 1: Payment PREPARE → failed (non-2xx or client error)");

		boolean bothPrepared = orderPrepare.success() && paymentPrepare.success();
		TwoPcStructuredLog.info(
				log,
				transactionId,
				"Phase 1 complete",
				"orderPrepareOk=" + orderPrepare.success() + " | paymentPrepareOk=" + paymentPrepare.success());

		String orderStatus;
		String paymentStatus;
		String overallResult;

		if (!bothPrepared) {
			TwoPcStructuredLog.warn(log, transactionId, "Decision", "prepare not unanimous → Phase 2 ROLLBACK both");
			addElapsed(timeline, t0, "Decision: ABORT — entering Phase 2 rollback (both participants)");

			TwoPcStructuredLog.info(log, transactionId, "Phase 2b outbound", "POST order rollback");
			addElapsed(timeline, t0, "Phase 2b: sending Order ROLLBACK");
			orderStatus = postRollback(orderServiceUrl, transactionId);
			addElapsed(timeline, t0, "Phase 2b: Order ROLLBACK → completed (participant status=" + orderStatus + ")");

			TwoPcStructuredLog.info(log, transactionId, "Phase 2b outbound", "POST payment rollback");
			addElapsed(timeline, t0, "Phase 2b: sending Payment ROLLBACK");
			paymentStatus = postRollback(paymentServiceUrl, transactionId);
			addElapsed(
					timeline,
					t0,
					"Phase 2b: Payment ROLLBACK → completed (participant status=" + paymentStatus + ")");

			overallResult = "ROLLED_BACK";
			transactionStates.put(transactionId, "ROLLED_BACK");
			addElapsed(timeline, t0, "Final outcome: ROLLED_BACK");
		} else {
			TwoPcStructuredLog.info(log, transactionId, "Decision", "both prepared → Phase 2 COMMIT path");
			addElapsed(timeline, t0, "Decision: COMMIT — entering Phase 2 commit (order first, then payment)");

			String orderCommitUrl = withFailParam(orderServiceUrl + "/commit", flags.isOrderCommitFail());
			TwoPcStructuredLog.info(log, transactionId, "Phase 2a outbound", "POST order commit -> " + orderCommitUrl);
			addElapsed(timeline, t0, "Phase 2a: sending Order COMMIT → " + orderCommitUrl);
			Phase2Result orderCommit = exchangePost(orderCommitUrl, "commit-order", transactionId);
			addElapsed(
					timeline,
					t0,
					orderCommit.success()
							? "Phase 2a: Order COMMIT → HTTP 2xx (status=" + orderCommit.status() + ")"
							: "Phase 2a: Order COMMIT → failed");

			if (!orderCommit.success()) {
				TwoPcStructuredLog.warn(
						log,
						transactionId,
						"Phase 2a abort",
						"order commit failed → rolling back both (skipping payment commit)");
				addElapsed(timeline, t0, "Compensation: Order COMMIT failed → ROLLBACK both participants");

				orderStatus = postRollback(orderServiceUrl, transactionId);
				addElapsed(timeline, t0, "Rollback: Order → status=" + orderStatus);
				paymentStatus = postRollback(paymentServiceUrl, transactionId);
				addElapsed(timeline, t0, "Rollback: Payment → status=" + paymentStatus);

				overallResult = "ROLLED_BACK";
				transactionStates.put(transactionId, "ROLLED_BACK");
				addElapsed(timeline, t0, "Final outcome: ROLLED_BACK (after commit failure)");
			} else {
				String paymentCommitUrl = withFailParam(paymentServiceUrl + "/commit", flags.isPaymentCommitFail());
				TwoPcStructuredLog.info(
						log,
						transactionId,
						"Phase 2a outbound",
						"POST payment commit -> " + paymentCommitUrl);
				addElapsed(timeline, t0, "Phase 2a: sending Payment COMMIT → " + paymentCommitUrl);
				Phase2Result paymentCommit = exchangePost(paymentCommitUrl, "commit-payment", transactionId);
				addElapsed(
						timeline,
						t0,
						paymentCommit.success()
								? "Phase 2a: Payment COMMIT → HTTP 2xx (status=" + paymentCommit.status() + ")"
								: "Phase 2a: Payment COMMIT → failed");

				if (!paymentCommit.success()) {
					TwoPcStructuredLog.warn(
							log,
							transactionId,
							"Phase 2a abort",
							"payment commit failed → rolling back both");
					addElapsed(timeline, t0, "Compensation: Payment COMMIT failed → ROLLBACK both participants");

					orderStatus = postRollback(orderServiceUrl, transactionId);
					addElapsed(timeline, t0, "Rollback: Order → status=" + orderStatus);
					paymentStatus = postRollback(paymentServiceUrl, transactionId);
					addElapsed(timeline, t0, "Rollback: Payment → status=" + paymentStatus);

					overallResult = "ROLLED_BACK";
					transactionStates.put(transactionId, "ROLLED_BACK");
					addElapsed(timeline, t0, "Final outcome: ROLLED_BACK (after payment commit failure)");
				} else {
					orderStatus = orderCommit.status();
					paymentStatus = paymentCommit.status();
					overallResult = "COMMITTED";
					transactionStates.put(transactionId, "COMMITTED");
					addElapsed(timeline, t0, "Final outcome: COMMITTED (both participants committed)");
				}
			}
		}

		TwoPcStructuredLog.info(
				log,
				transactionId,
				"TRANSACTION finished",
				"overallResult=" + overallResult + " | orderStatus=" + orderStatus + " | paymentStatus=" + paymentStatus);

		TwoPcStructuredLog.logTransactionSummary(log, transactionId, timeline, overallResult, orderStatus, paymentStatus);

		return new StartTransactionResponse(transactionId, orderStatus, paymentStatus, overallResult);
	}

	private static void addElapsed(List<String> timeline, long t0, String message) {
		long ms = (System.nanoTime() - t0) / 1_000_000L;
		timeline.add(String.format("+%d ms — %s", ms, message));
	}

	private static String withFailParam(String url, boolean fail) {
		if (!fail) {
			return url;
		}
		return url + (url.contains("?") ? "&" : "?") + "fail=true";
	}

	private HttpEntity<Void> txEntity(String transactionId) {
		HttpHeaders headers = new HttpHeaders();
		headers.set(TRANSACTION_ID_HEADER, transactionId);
		return new HttpEntity<>(headers);
	}

	private PrepareOutcome postPrepare(String url, String transactionId) {
		Phase2Result result = exchangePost(url, "prepare", transactionId);
		return new PrepareOutcome(result.success());
	}

	private String postRollback(String baseUrl, String transactionId) {
		String url = baseUrl + "/rollback";
		Phase2Result result = exchangePost(url, "rollback", transactionId);
		return result.status();
	}

	private Phase2Result exchangePost(String url, String logLabel, String transactionId) {
		TwoPcStructuredLog.info(log, transactionId, "HTTP outbound", logLabel + " → " + url);
		try {
			ResponseEntity<ParticipantTransactionResponse> response = restTemplate.exchange(
					url,
					HttpMethod.POST,
					txEntity(transactionId),
					ParticipantTransactionResponse.class);
			if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
				TwoPcStructuredLog.info(
						log,
						transactionId,
						"HTTP response",
						logLabel + " ← " + response.getStatusCode() + " body.status=" + response.getBody().status());
				return new Phase2Result(true, response.getBody().status());
			}
			TwoPcStructuredLog.warn(
					log,
					transactionId,
					"HTTP response",
					logLabel + " ← non-success " + response.getStatusCode() + " url=" + url);
			return new Phase2Result(false, "UNKNOWN");
		} catch (RestClientException e) {
			TwoPcStructuredLog.warn(
					log,
					transactionId,
					"HTTP error",
					logLabel + " failed url=" + url + " — " + e.getMessage());
			return new Phase2Result(false, "ERROR");
		}
	}

	private record PrepareOutcome(boolean success) {
	}

	private record Phase2Result(boolean success, String status) {
	}
}
