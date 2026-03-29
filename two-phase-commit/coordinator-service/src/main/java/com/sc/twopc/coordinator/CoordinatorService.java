package com.sc.twopc.coordinator;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Service
public class CoordinatorService {

	private static final Logger log = LoggerFactory.getLogger(CoordinatorService.class);

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
		log.info("{} | tx={} | 2PC transaction started", Instant.now(), transactionId);
		transactionStates.put(transactionId, "PREPARING");

		String orderPrepareUrl = withFailParam(orderServiceUrl + "/prepare", flags.isOrderPrepareFail());
		log.info("{} | tx={} | Phase 1: POST order prepare -> {}", Instant.now(), transactionId, orderPrepareUrl);
		PrepareOutcome orderPrepare = postPrepare(orderPrepareUrl, transactionId);

		String paymentPrepareUrl = withFailParam(paymentServiceUrl + "/prepare", flags.isPaymentPrepareFail());
		log.info("{} | tx={} | Phase 1: POST payment prepare -> {}", Instant.now(), transactionId, paymentPrepareUrl);
		PrepareOutcome paymentPrepare = postPrepare(paymentPrepareUrl, transactionId);

		boolean bothPrepared = orderPrepare.success() && paymentPrepare.success();
		log.info(
				"{} | tx={} | Phase 1 complete | orderOk={} paymentOk={}",
				Instant.now(),
				transactionId,
				orderPrepare.success(),
				paymentPrepare.success());

		String orderStatus;
		String paymentStatus;
		String overallResult;

		if (!bothPrepared) {
			log.warn(
					"{} | tx={} | Phase 1 failed | rolling back both participants",
					Instant.now(),
					transactionId);
			log.info("{} | tx={} | Phase 2b: rollback order", Instant.now(), transactionId);
			orderStatus = postRollback(orderServiceUrl, transactionId);
			log.info("{} | tx={} | Phase 2b: rollback payment", Instant.now(), transactionId);
			paymentStatus = postRollback(paymentServiceUrl, transactionId);
			overallResult = "ROLLED_BACK";
			transactionStates.put(transactionId, "ROLLED_BACK");
		} else {
			String orderCommitUrl = withFailParam(orderServiceUrl + "/commit", flags.isOrderCommitFail());
			log.info("{} | tx={} | Phase 2a: POST order commit -> {}", Instant.now(), transactionId, orderCommitUrl);
			Phase2Result orderCommit = exchangePost(orderCommitUrl, "commit-order", transactionId);

			if (!orderCommit.success()) {
				log.warn(
						"{} | tx={} | order commit failed | rolling back both participants",
						Instant.now(),
						transactionId);
				orderStatus = postRollback(orderServiceUrl, transactionId);
				paymentStatus = postRollback(paymentServiceUrl, transactionId);
				overallResult = "ROLLED_BACK";
				transactionStates.put(transactionId, "ROLLED_BACK");
			} else {
				String paymentCommitUrl = withFailParam(paymentServiceUrl + "/commit", flags.isPaymentCommitFail());
				log.info(
						"{} | tx={} | Phase 2a: POST payment commit -> {}",
						Instant.now(),
						transactionId,
						paymentCommitUrl);
				Phase2Result paymentCommit = exchangePost(paymentCommitUrl, "commit-payment", transactionId);

				if (!paymentCommit.success()) {
					log.warn(
							"{} | tx={} | payment commit failed | rolling back both participants",
							Instant.now(),
							transactionId);
					orderStatus = postRollback(orderServiceUrl, transactionId);
					paymentStatus = postRollback(paymentServiceUrl, transactionId);
					overallResult = "ROLLED_BACK";
					transactionStates.put(transactionId, "ROLLED_BACK");
				} else {
					orderStatus = orderCommit.status();
					paymentStatus = paymentCommit.status();
					overallResult = "COMMITTED";
					transactionStates.put(transactionId, "COMMITTED");
				}
			}
		}

		log.info(
				"{} | tx={} | 2PC finished | overallResult={} | orderStatus={} | paymentStatus={}",
				Instant.now(),
				transactionId,
				overallResult,
				orderStatus,
				paymentStatus);

		return new StartTransactionResponse(transactionId, orderStatus, paymentStatus, overallResult);
	}

	private static String withFailParam(String url, boolean fail) {
		if (!fail) {
			return url;
		}
		return url + (url.contains("?") ? "&" : "?") + "fail=true";
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
		log.info("{} | tx={} | {} -> {}", Instant.now(), transactionId, logLabel, url);
		try {
			ResponseEntity<ParticipantTransactionResponse> response = restTemplate.exchange(
					url,
					HttpMethod.POST,
					HttpEntity.EMPTY,
					ParticipantTransactionResponse.class);
			if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
				return new Phase2Result(true, response.getBody().status());
			}
			log.warn(
					"{} | tx={} | {} non-success HTTP | url={} | status={}",
					Instant.now(),
					transactionId,
					logLabel,
					url,
					response.getStatusCode());
			return new Phase2Result(false, "UNKNOWN");
		} catch (RestClientException e) {
			log.warn(
					"{} | tx={} | {} request failed | url={} | {}",
					Instant.now(),
					transactionId,
					logLabel,
					url,
					e.getMessage());
			return new Phase2Result(false, "ERROR");
		}
	}

	private record PrepareOutcome(boolean success) {
	}

	private record Phase2Result(boolean success, String status) {
	}
}
