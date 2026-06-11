package com.delivery.ms.service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.delivery.ms.entity.Delivery;
import com.delivery.ms.entity.DeliveryRepository;
import com.sc.saga.EventStatus;
import com.sc.saga.SagaEvent;
import com.sc.saga.SagaEventType;
import com.sc.saga.SagaTopics;

@Service
public class DeliverySagaService {

	private static final Logger log = LoggerFactory.getLogger(DeliverySagaService.class);

	private static final int DELIVERY_FAILURE_MIN_QUANTITY = 5_000;

	private final DeliveryRepository deliveryRepository;
	private final KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate;
	private final DeliverySagaService self;

	public DeliverySagaService(
			DeliveryRepository deliveryRepository,
			KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate,
			@Lazy DeliverySagaService self) {
		this.deliveryRepository = deliveryRepository;
		this.sagaEventKafkaTemplate = sagaEventKafkaTemplate;
		this.self = self;
	}

	@KafkaListener(
			topics = SagaTopics.DELIVERY_EVENTS,
			containerFactory = "sagaEventKafkaListenerContainerFactory",
			groupId = "delivery-saga-participant")
	public void onDeliverySagaEvent(SagaEvent event) {
		Instant now = Instant.now();
		switch (event.getEventType()) {
			case DELIVERY_REQUESTED -> {
				log.info("{} | orderId={} action=DELIVERY_REQUEST_RECEIVED", now, event.getOrderId());
				self.handleDeliveryRequested(event);
			}
			case DELIVERY_CANCELLED -> {
				log.info("{} | orderId={} action=DELIVERY_CANCEL_RECEIVED", now, event.getOrderId());
				self.handleDeliveryCancelled(event);
			}
			default -> log.debug("{} | orderId={} action=IGNORE_DELIVERY_EVENT type={}", now, event.getOrderId(), event.getEventType());
		}
	}

	@Transactional
	protected void handleDeliveryRequested(SagaEvent event) {
		Instant now = Instant.now();
		Long orderId = event.getOrderId();
		Map<String, Object> p = event.getPayload();
		int quantity = intValue(p.get("quantity"), 0);
		String address = p.get("address") != null ? p.get("address").toString() : "default-warehouse";

		if (orderId == null) {
			log.warn("{} | action=DELIVERY_REQUEST_INVALID no orderId", now);
			return;
		}

		if (failAtStep(p, "DELIVERY")) {
			publishFailed(orderId, p, "simulated failure (failAt=DELIVERY)");
			log.warn("{} | orderId={} action=DELIVERY_FAIL_SIMULATED failAt=DELIVERY", Instant.now(), orderId);
			return;
		}

		if (quantity >= DELIVERY_FAILURE_MIN_QUANTITY) {
			Delivery shipment = new Delivery();
			shipment.setOrderId(orderId);
			shipment.setAddress(address);
			shipment.setStatus("failed");
			deliveryRepository.save(shipment);
			publishFailed(orderId, p, "quantity exceeds simulated delivery capacity");
			log.warn(
					"{} | orderId={} action=DELIVERY_FAILED_SIMULATED quantity>={}",
					Instant.now(),
					orderId,
					DELIVERY_FAILURE_MIN_QUANTITY);
			return;
		}

		Delivery shipment = new Delivery();
		shipment.setOrderId(orderId);
		shipment.setAddress(address);
		shipment.setStatus("scheduled");
		deliveryRepository.save(shipment);

		Map<String, Object> out = copyPayload(p);
		SagaEvent ok = SagaEvent.of(orderId, SagaEventType.DELIVERY_SCHEDULED, EventStatus.SUCCESS, out);
		sagaEventKafkaTemplate.send(SagaTopics.DELIVERY_EVENTS, ok);
		log.info("{} | orderId={} action=DELIVERY_SCHEDULED address={} deliveryId={}", Instant.now(), orderId, address, shipment.getId());
	}

	@Transactional
	protected void handleDeliveryCancelled(SagaEvent event) {
		Instant now = Instant.now();
		Long orderId = event.getOrderId();
		if (orderId == null) {
			log.warn("{} | action=DELIVERY_CANCEL_SKIP no orderId", now);
			return;
		}
		deliveryRepository.findFirstByOrderId(orderId).ifPresentOrElse(
				shipment -> {
					shipment.setStatus("cancelled");
					deliveryRepository.save(shipment);
					log.info("{} | orderId={} action=DELIVERY_CANCEL_APPLIED deliveryId={}", Instant.now(), orderId, shipment.getId());
				},
				() -> log.warn("{} | orderId={} action=DELIVERY_CANCEL_SKIP_NO_ROW", now, orderId));
	}

	private void publishFailed(Long orderId, Map<String, Object> originalPayload, String reason) {
		Map<String, Object> out = copyPayload(originalPayload);
		out.put("reason", reason);
		SagaEvent fail = SagaEvent.of(orderId, SagaEventType.DELIVERY_FAILED, EventStatus.FAILED, out);
		sagaEventKafkaTemplate.send(SagaTopics.DELIVERY_EVENTS, fail);
		log.info("{} | orderId={} action=DELIVERY_FAILED_PUBLISHED reason={}", Instant.now(), orderId, reason);
	}

	private static Map<String, Object> copyPayload(Map<String, Object> p) {
		return p != null ? new HashMap<>(p) : new HashMap<>();
	}

	private static int intValue(Object o, int defaultVal) {
		if (o == null) {
			return defaultVal;
		}
		if (o instanceof Number n) {
			return n.intValue();
		}
		return Integer.parseInt(o.toString());
	}

	private static boolean failAtStep(Map<String, Object> payload, String step) {
		Object v = payload != null ? payload.get("failAt") : null;
		if (v == null) {
			return false;
		}
		return step.equalsIgnoreCase(v.toString().trim());
	}
}
