package com.stock.ms.service;

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

import com.sc.saga.EventStatus;
import com.sc.saga.SagaEvent;
import com.sc.saga.SagaEventType;
import com.sc.saga.SagaTopics;
import com.stock.ms.entity.InventoryStockRepository;
import com.stock.ms.entity.Stock;

@Service
public class StockSagaService {

	private static final Logger log = LoggerFactory.getLogger(StockSagaService.class);

	private final InventoryStockRepository inventoryStockRepository;
	private final KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate;
	private final StockSagaService self;

	public StockSagaService(
			InventoryStockRepository inventoryStockRepository,
			KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate,
			@Lazy StockSagaService self) {
		this.inventoryStockRepository = inventoryStockRepository;
		this.sagaEventKafkaTemplate = sagaEventKafkaTemplate;
		this.self = self;
	}

	@KafkaListener(
			topics = SagaTopics.ORDER_EVENTS,
			containerFactory = "sagaEventKafkaListenerContainerFactory",
			groupId = "stock-saga-participant-order")
	public void onOrderEvent(SagaEvent event) {
		Instant now = Instant.now();
		if (event.getEventType() != SagaEventType.ORDER_CREATED) {
			log.debug("{} | orderId={} action=IGNORE_ORDER_EVENT type={}", now, event.getOrderId(), event.getEventType());
			return;
		}
		Long orderId = event.getOrderId();
		Map<String, Object> p = event.getPayload();
		long productId = longValue(p.get("productId"), -1L);
		int qty = intValue(p.get("quantity"), 0);
		log.info("{} | orderId={} action=ORDER_RECEIVED productId={} quantity={}", now, orderId, productId, qty);

		if (orderId == null || productId < 0 || qty <= 0) {
			log.warn(
					"{} | orderId={} action=ORDER_CREATED_INVALID productId={} quantity={}",
					Instant.now(),
					orderId,
					productId,
					qty);
			if (orderId != null) {
				publishFailed(orderId, p, "invalid payload");
			}
			return;
		}

		self.reserveAndPublish(orderId, productId, qty, p);
	}

	@KafkaListener(
			topics = SagaTopics.STOCK_EVENTS,
			containerFactory = "sagaEventKafkaListenerContainerFactory",
			groupId = "stock-saga-participant-stock")
	public void onStockEvent(SagaEvent event) {
		Instant now = Instant.now();
		if (event.getEventType() != SagaEventType.STOCK_RELEASED) {
			log.debug("{} | orderId={} action=IGNORE_STOCK_EVENT type={}", now, event.getOrderId(), event.getEventType());
			return;
		}
		Long orderId = event.getOrderId();
		Map<String, Object> p = event.getPayload();
		long productId = longValue(p.get("productId"), -1L);
		int qty = intValue(p.get("quantity"), 0);
		log.info("{} | orderId={} action=STOCK_RELEASE_COMPENSATION productId={} quantity={}", now, orderId, productId, qty);

		if (orderId == null || productId < 0 || qty <= 0) {
			log.warn("{} | orderId={} action=STOCK_RELEASE_SKIP invalid payload", Instant.now(), orderId);
			return;
		}

		self.releaseStock(productId, qty);
	}

	@Transactional
	protected void reserveAndPublish(Long orderId, long productId, int qty, Map<String, Object> originalPayload) {
		Instant now = Instant.now();
		if (failAtStep(originalPayload, "STOCK")) {
			publishFailed(orderId, originalPayload, "simulated failure (failAt=STOCK)");
			log.warn("{} | orderId={} action=STOCK_FAIL_SIMULATED failAt=STOCK", Instant.now(), orderId);
			return;
		}
		Stock row = inventoryStockRepository.findByProductId(productId).orElse(null);
		if (row == null) {
			log.warn("{} | orderId={} action=STOCK_NOT_FOUND productId={}", now, orderId, productId);
			publishFailed(orderId, originalPayload, "unknown product");
			return;
		}
		if (row.getAvailableQuantity() < qty) {
			log.warn(
					"{} | orderId={} action=STOCK_INSUFFICIENT productId={} need={} have={}",
					now,
					orderId,
					productId,
					qty,
					row.getAvailableQuantity());
			publishFailed(orderId, originalPayload, "insufficient stock");
			return;
		}
		row.setAvailableQuantity(row.getAvailableQuantity() - qty);
		inventoryStockRepository.save(row);
		Map<String, Object> out = copyPayload(originalPayload);
		SagaEvent ok = SagaEvent.of(orderId, SagaEventType.STOCK_RESERVED, EventStatus.SUCCESS, out);
		sagaEventKafkaTemplate.send(SagaTopics.STOCK_EVENTS, ok);
		log.info(
				"{} | orderId={} action=STOCK_RESERVED productId={} reserved={} remaining={}",
				Instant.now(),
				orderId,
				productId,
				qty,
				row.getAvailableQuantity());
	}

	@Transactional
	protected void releaseStock(long productId, int qty) {
		Instant now = Instant.now();
		inventoryStockRepository.findByProductId(productId).ifPresentOrElse(
				row -> {
					row.setAvailableQuantity(row.getAvailableQuantity() + qty);
					inventoryStockRepository.save(row);
					log.info(
							"{} | orderId=n/a action=STOCK_RELEASED_APPLIED productId={} released={} newAvailable={}",
							Instant.now(),
							productId,
							qty,
							row.getAvailableQuantity());
				},
				() -> log.warn("{} | action=STOCK_RELEASE_SKIP_NO_ROW productId={}", now, productId));
	}

	private void publishFailed(Long orderId, Map<String, Object> originalPayload, String reason) {
		Map<String, Object> out = copyPayload(originalPayload);
		out.put("reason", reason);
		SagaEvent fail = SagaEvent.of(orderId, SagaEventType.STOCK_RESERVE_FAILED, EventStatus.FAILED, out);
		sagaEventKafkaTemplate.send(SagaTopics.STOCK_EVENTS, fail);
		log.info("{} | orderId={} action=STOCK_RESERVE_FAILED reason={}", Instant.now(), orderId, reason);
	}

	private static Map<String, Object> copyPayload(Map<String, Object> p) {
		return p != null ? new HashMap<>(p) : new HashMap<>();
	}

	private static long longValue(Object o, long defaultVal) {
		if (o == null) {
			return defaultVal;
		}
		if (o instanceof Number n) {
			return n.longValue();
		}
		return Long.parseLong(o.toString());
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
