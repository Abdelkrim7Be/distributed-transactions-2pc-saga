package com.payment.ms.service;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.payment.ms.entity.Payment;
import com.payment.ms.entity.PaymentRepository;
import com.sc.saga.EventStatus;
import com.sc.saga.SagaEvent;
import com.sc.saga.SagaEventType;
import com.sc.saga.SagaTopics;

@Service
public class PaymentSagaService {

	private static final Logger log = LoggerFactory.getLogger(PaymentSagaService.class);

	/** Deterministic failure for demos: refuse very large amounts. */
	private static final double PAYMENT_FAILURE_THRESHOLD = 500_000d;

	private final PaymentRepository paymentRepository;
	private final KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate;
	private final PaymentSagaService self;

	public PaymentSagaService(
			PaymentRepository paymentRepository,
			KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate,
			@Lazy PaymentSagaService self) {
		this.paymentRepository = paymentRepository;
		this.sagaEventKafkaTemplate = sagaEventKafkaTemplate;
		this.self = self;
	}

	@KafkaListener(
			topics = SagaTopics.PAYMENT_EVENTS,
			containerFactory = "sagaEventKafkaListenerContainerFactory",
			groupId = "payment-saga-participant")
	public void onPaymentSagaEvent(SagaEvent event) {
		Instant now = Instant.now();
		switch (event.getEventType()) {
			case PAYMENT_REQUESTED -> {
				log.info("{} | orderId={} action=PAYMENT_REQUEST_RECEIVED", now, event.getOrderId());
				self.handlePaymentRequested(event);
			}
			case PAYMENT_REFUNDED -> {
				log.info("{} | orderId={} action=PAYMENT_REFUND_RECEIVED", now, event.getOrderId());
				self.handlePaymentRefunded(event);
			}
			default -> log.debug("{} | orderId={} action=IGNORE_PAYMENT_EVENT type={}", now, event.getOrderId(), event.getEventType());
		}
	}

	@Transactional
	protected void handlePaymentRequested(SagaEvent event) {
		Instant now = Instant.now();
		Long orderId = event.getOrderId();
		Map<String, Object> p = event.getPayload();
		double amount = doubleValue(p.get("amount"), -1d);
		String mode = p.get("paymentMode") != null ? p.get("paymentMode").toString() : "CARD";

		if (orderId == null || amount < 0) {
			log.warn("{} | orderId={} action=PAYMENT_REQUEST_INVALID amount={}", now, orderId, amount);
			if (orderId != null) {
				publishFailed(orderId, p, "invalid payment payload");
			}
			return;
		}

		if (amount >= PAYMENT_FAILURE_THRESHOLD) {
			Payment payment = new Payment();
			payment.setOrderId(orderId);
			payment.setAmount(amount);
			payment.setMode(mode);
			payment.setStatus("FAILED");
			paymentRepository.save(payment);
			publishFailed(orderId, p, "amount exceeds simulated limit");
			log.warn("{} | orderId={} action=PAYMENT_FAILED_SIMULATED amount>={}", now, orderId, PAYMENT_FAILURE_THRESHOLD);
			return;
		}

		Payment payment = new Payment();
		payment.setOrderId(orderId);
		payment.setAmount(amount);
		payment.setMode(mode);
		payment.setStatus("SUCCESS");
		paymentRepository.save(payment);

		Map<String, Object> out = copyPayload(p);
		SagaEvent ok = SagaEvent.of(orderId, SagaEventType.PAYMENT_PROCESSED, EventStatus.SUCCESS, out);
		sagaEventKafkaTemplate.send(SagaTopics.PAYMENT_EVENTS, ok);
		log.info("{} | orderId={} action=PAYMENT_PROCESSED amount={} mode={}", Instant.now(), orderId, amount, mode);
	}

	@Transactional
	protected void handlePaymentRefunded(SagaEvent event) {
		Instant now = Instant.now();
		Long orderId = event.getOrderId();
		if (orderId == null) {
			log.warn("{} | action=PAYMENT_REFUND_SKIP no orderId", now);
			return;
		}
		List<Payment> payments = paymentRepository.findByOrderId(orderId);
		if (payments.isEmpty()) {
			log.warn("{} | orderId={} action=PAYMENT_REFUND_SKIP_NO_PAYMENT", now, orderId);
			return;
		}
		Payment payment = payments.get(0);
		payment.setStatus("REFUNDED");
		paymentRepository.save(payment);
		log.info("{} | orderId={} action=PAYMENT_REFUND_APPLIED paymentId={}", Instant.now(), orderId, payment.getId());
	}

	private void publishFailed(Long orderId, Map<String, Object> originalPayload, String reason) {
		Map<String, Object> out = copyPayload(originalPayload);
		out.put("reason", reason);
		SagaEvent fail = SagaEvent.of(orderId, SagaEventType.PAYMENT_FAILED, EventStatus.FAILED, out);
		sagaEventKafkaTemplate.send(SagaTopics.PAYMENT_EVENTS, fail);
		log.info("{} | orderId={} action=PAYMENT_FAILED_PUBLISHED reason={}", Instant.now(), orderId, reason);
	}

	private static Map<String, Object> copyPayload(Map<String, Object> p) {
		return p != null ? new HashMap<>(p) : new HashMap<>();
	}

	private static double doubleValue(Object o, double defaultVal) {
		if (o == null) {
			return defaultVal;
		}
		if (o instanceof Number n) {
			return n.doubleValue();
		}
		return Double.parseDouble(o.toString());
	}
}
