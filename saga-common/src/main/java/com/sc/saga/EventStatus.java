package com.sc.saga;

/**
 * Outcome of a saga step as carried on {@link SagaEvent}.
 */
public enum EventStatus {
	SUCCESS,
	FAILED
}
