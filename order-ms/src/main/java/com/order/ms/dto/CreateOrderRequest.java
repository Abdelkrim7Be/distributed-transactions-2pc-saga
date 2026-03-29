package com.order.ms.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CreateOrderRequest {

	private Long productId;
	private int quantity;
	private double amount;

	/** Optional: {@code STOCK}, {@code PAYMENT}, or {@code DELIVERY} to force failure at that step. */
	private String failAt;
}
