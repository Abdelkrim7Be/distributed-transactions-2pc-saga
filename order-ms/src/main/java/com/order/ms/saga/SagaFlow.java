package com.order.ms.saga;

import java.util.List;

import com.sc.saga.SagaEventType;
import com.sc.saga.SagaTopics;

/**
 * Orchestrated saga design (order-ms is the entry point).
 *
 * <p><b>Forward flow</b>
 * <ol>
 *   <li>Customer places order → Order Service persists {@code CREATED}</li>
 *   <li>Order Service publishes success on {@link SagaTopics#ORDER_EVENTS} ({@link SagaEventType#ORDER_CREATED})</li>
 *   <li>Stock Service reserves inventory → {@link SagaTopics#STOCK_EVENTS} ({@link SagaEventType#STOCK_RESERVED})</li>
 *   <li>Payment Service charges → {@link SagaTopics#PAYMENT_EVENTS} ({@link SagaEventType#PAYMENT_PROCESSED})</li>
 *   <li>Delivery Service schedules shipment → {@link SagaTopics#DELIVERY_EVENTS} ({@link SagaEventType#DELIVERY_SCHEDULED})</li>
 * </ol>
 *
 * <p><b>Compensation</b> — if any step fails, run compensating transactions in <b>reverse</b> order
 * (cancel delivery → refund payment → release stock → mark order failed).
 */
public final class SagaFlow {

	private SagaFlow() {
	}

	/** Topics in forward pipeline order (after local order create). */
	public static final List<String> FORWARD_TOPIC_CHAIN = List.of(
			SagaTopics.ORDER_EVENTS,
			SagaTopics.STOCK_EVENTS,
			SagaTopics.PAYMENT_EVENTS,
			SagaTopics.DELIVERY_EVENTS);

	/** Topics visited in reverse when compensating (newest participating service first). */
	public static final List<String> COMPENSATION_TOPIC_CHAIN = List.of(
			SagaTopics.DELIVERY_EVENTS,
			SagaTopics.PAYMENT_EVENTS,
			SagaTopics.STOCK_EVENTS,
			SagaTopics.ORDER_EVENTS);

	public enum ForwardStep {
		ORDER_CREATED(SagaTopics.ORDER_EVENTS, SagaEventType.ORDER_CREATED, "Order persisted with status CREATED"),
		STOCK_RESERVED(SagaTopics.STOCK_EVENTS, SagaEventType.STOCK_RESERVED, "Inventory reserved for the order"),
		PAYMENT_PROCESSED(SagaTopics.PAYMENT_EVENTS, SagaEventType.PAYMENT_PROCESSED, "Payment captured successfully"),
		DELIVERY_SCHEDULED(SagaTopics.DELIVERY_EVENTS, SagaEventType.DELIVERY_SCHEDULED, "Delivery scheduled");

		private final String topic;
		private final SagaEventType successEvent;
		private final String description;

		ForwardStep(String topic, SagaEventType successEvent, String description) {
			this.topic = topic;
			this.successEvent = successEvent;
			this.description = description;
		}

		public String topic() {
			return topic;
		}

		public SagaEventType successEvent() {
			return successEvent;
		}

		public String description() {
			return description;
		}
	}

	public enum CompensationStep {
		CANCEL_DELIVERY(SagaTopics.DELIVERY_EVENTS, SagaEventType.DELIVERY_CANCELLED),
		REFUND_PAYMENT(SagaTopics.PAYMENT_EVENTS, SagaEventType.PAYMENT_REFUNDED),
		RELEASE_STOCK(SagaTopics.STOCK_EVENTS, SagaEventType.STOCK_RELEASED),
		MARK_ORDER_FAILED(SagaTopics.ORDER_EVENTS, SagaEventType.ORDER_FAILED);

		private final String topic;
		private final SagaEventType eventType;

		CompensationStep(String topic, SagaEventType eventType) {
			this.topic = topic;
			this.eventType = eventType;
		}

		public String topic() {
			return topic;
		}

		public SagaEventType eventType() {
			return eventType;
		}
	}
}
