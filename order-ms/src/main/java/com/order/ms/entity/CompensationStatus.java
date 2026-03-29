package com.order.ms.entity;

/**
 * Tracks which compensating messages the orchestrator has published for an order.
 */
public enum CompensationStatus {

	/** No compensation has been started. */
	NONE,
	/** Compensation flow started; publishing events in reverse saga order. */
	IN_PROGRESS,
	/** {@code DELIVERY_CANCELLED} published (when applicable). */
	DELIVERY_CANCEL_PUBLISHED,
	/** {@code PAYMENT_REFUNDED} published (when applicable). */
	PAYMENT_REFUND_PUBLISHED,
	/** {@code STOCK_RELEASED} published (when applicable). */
	STOCK_RELEASE_PUBLISHED,
	/** All applicable compensation events and {@code ORDER_FAILED} have been published. */
	COMPLETED
}
