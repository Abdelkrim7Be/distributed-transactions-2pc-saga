package com.sc.saga;

/**
 * Kafka topic names for the orchestrated saga.
 */
public final class SagaTopics {

	public static final String ORDER_EVENTS = "order-events";
	public static final String STOCK_EVENTS = "stock-events";
	public static final String PAYMENT_EVENTS = "payment-events";
	public static final String DELIVERY_EVENTS = "delivery-events";

	private SagaTopics() {
	}
}
