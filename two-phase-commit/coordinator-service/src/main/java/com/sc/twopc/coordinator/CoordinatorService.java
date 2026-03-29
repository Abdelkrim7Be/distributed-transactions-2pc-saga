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

	public StartTransactionResponse startTransaction() {
		String transactionId = UUID.randomUUID().toString();
		log.info("{} | tx={} | 2PC transaction started", Instant.now(), transactionId);
		transactionStates.put(transactionId, "PREPARING");

		log.info("{} | tx={} | Phase 1: POST order prepare -> {}", Instant.now(), transactionId, orderServiceUrl + "/prepare");
		PrepareOutcome orderPrepare = postPrepare(orderServiceUrl);

		log.info("{} | tx={} | Phase 1: POST payment prepare -> {}", Instant.now(), transactionId, paymentServiceUrl + "/prepare");
		PrepareOutcome paymentPrepare = postPrepare(paymentServiceUrl);

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

		if (bothPrepared) {
			log.info("{} | tx={} | Phase 2a: commit both participants", Instant.now(), transactionId);
			orderStatus = postPhase2(orderServiceUrl, "commit", transactionId);
			paymentStatus = postPhase2(paymentServiceUrl, "commit", transactionId);
			overallResult = "COMMITTED";
			transactionStates.put(transactionId, "COMMITTED");
		} else {
			log.warn(
					"{} | tx={} | Phase 1 failed | rolling back both participants",
					Instant.now(),
					transactionId);
			log.info("{} | tx={} | Phase 2b: rollback order", Instant.now(), transactionId);
			orderStatus = postPhase2(orderServiceUrl, "rollback", transactionId);
			log.info("{} | tx={} | Phase 2b: rollback payment", Instant.now(), transactionId);
			paymentStatus = postPhase2(paymentServiceUrl, "rollback", transactionId);
			overallResult = "ROLLED_BACK";
			transactionStates.put(transactionId, "ROLLED_BACK");
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

	private PrepareOutcome postPrepare(String baseUrl) {
		String url = baseUrl + "/prepare";
		try {
			ResponseEntity<ParticipantTransactionResponse> response = restTemplate.exchange(
					url,
					HttpMethod.POST,
					HttpEntity.EMPTY,
					ParticipantTransactionResponse.class);
			if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
				return new PrepareOutcome(true);
			}
			log.warn("{} | prepare non-success HTTP | url={} | status={}", Instant.now(), url, response.getStatusCode());
			return new PrepareOutcome(false);
		} catch (RestClientException e) {
			log.warn("{} | prepare request failed | url={} | {}", Instant.now(), url, e.getMessage());
			return new PrepareOutcome(false);
		}
	}

	private String postPhase2(String baseUrl, String action, String transactionId) {
		String url = baseUrl + "/" + action;
		log.info("{} | tx={} | POST {} -> {}", Instant.now(), transactionId, action, url);
		try {
			ResponseEntity<ParticipantTransactionResponse> response = restTemplate.exchange(
					url,
					HttpMethod.POST,
					HttpEntity.EMPTY,
					ParticipantTransactionResponse.class);
			if (response.getBody() != null) {
				return response.getBody().status();
			}
			log.warn("{} | tx={} | {} empty body | url={}", Instant.now(), transactionId, action, url);
			return "UNKNOWN";
		} catch (RestClientException e) {
			log.error("{} | tx={} | {} failed | url={} | {}", Instant.now(), transactionId, action, url, e.getMessage());
			return "ERROR";
		}
	}

	private record PrepareOutcome(boolean success) {
	}
}
