package com.sc.saga;

/**
 * All event types exchanged across the order → stock → payment → delivery saga.
 */
public enum SagaEventType {

	// order-events
	ORDER_CREATED,
	ORDER_COMPLETED,
	ORDER_FAILED,

	// stock-events
	STOCK_RESERVED,
	STOCK_RESERVE_FAILED,
	STOCK_RELEASED,

	// payment-events
	PAYMENT_PROCESSED,
	PAYMENT_FAILED,
	PAYMENT_REFUNDED,

	// delivery-events
	DELIVERY_SCHEDULED,
	DELIVERY_FAILED,
	DELIVERY_CANCELLED
}
