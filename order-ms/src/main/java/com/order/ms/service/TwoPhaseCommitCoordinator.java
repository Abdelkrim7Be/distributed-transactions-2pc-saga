package com.order.ms.service;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class TwoPhaseCommitCoordinator {

	private static final Logger log = LoggerFactory.getLogger(TwoPhaseCommitCoordinator.class);

	private final RestTemplate restTemplate = new RestTemplate();

	private final String STOCK_URL = "http://stock-ms:8082/api/2pc/stock";
	private final String PAYMENT_URL = "http://payment-ms:8081/api/2pc/payment";
	private final String DELIVERY_URL = "http://delivery-ms:8083/api/2pc/delivery";

	public boolean coordinate(Long orderId, Long productId, int quantity, double amount) {
		log.info("2PC START | orderId={}", orderId);

		Map<String, Object> stockReq = Map.of("orderId", orderId, "productId", productId, "quantity", quantity);
		Map<String, Object> paymentReq = Map.of("orderId", orderId, "amount", amount);
		Map<String, Object> deliveryReq = Map.of("orderId", orderId, "address", "2PC-Warehouse");

		try {
			// Phase 1: Prepare
			log.info("2PC PHASE 1: PREPARE | orderId={}", orderId);
			boolean stockPrepared = prepare(STOCK_URL, stockReq);
			boolean paymentPrepared = prepare(PAYMENT_URL, paymentReq);
			boolean deliveryPrepared = prepare(DELIVERY_URL, deliveryReq);

			if (stockPrepared && paymentPrepared && deliveryPrepared) {
				// Phase 2: Commit
				log.info("2PC PHASE 2: COMMIT | orderId={}", orderId);
				commit(STOCK_URL, stockReq);
				commit(PAYMENT_URL, paymentReq);
				commit(DELIVERY_URL, deliveryReq);
				log.info("2PC SUCCESS | orderId={}", orderId);
				return true;
			} else {
				// Phase 2: Rollback
				log.warn("2PC PHASE 2: ROLLBACK | orderId={} stock={} payment={} delivery={}", 
						orderId, stockPrepared, paymentPrepared, deliveryPrepared);
				rollback(STOCK_URL, stockReq);
				rollback(PAYMENT_URL, paymentReq);
				rollback(DELIVERY_URL, deliveryReq);
				return false;
			}
		} catch (Exception e) {
			log.error("2PC CRITICAL FAILURE | orderId={} error={}", orderId, e.getMessage());
			// In a real 2PC, we'd need a recovery log here
			return false;
		}
	}

	private boolean prepare(String baseUrl, Map<String, Object> req) {
		try {
			restTemplate.postForEntity(baseUrl + "/prepare", req, String.class);
			return true;
		} catch (Exception e) {
			log.warn("2PC PREPARE FAILED at {} | error={}", baseUrl, e.getMessage());
			return false;
		}
	}

	private void commit(String baseUrl, Map<String, Object> req) {
		try {
			restTemplate.postForEntity(baseUrl + "/commit", req, String.class);
		} catch (Exception e) {
			log.error("2PC COMMIT FAILED at {} | manual intervention required!", baseUrl);
		}
	}

	private void rollback(String baseUrl, Map<String, Object> req) {
		try {
			restTemplate.postForEntity(baseUrl + "/rollback", req, String.class);
		} catch (Exception e) {
			log.error("2PC ROLLBACK FAILED at {} | leaked resources possible", baseUrl);
		}
	}
}
