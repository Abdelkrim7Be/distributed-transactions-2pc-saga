package com.order.ms.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CreateOrderRequest {

	private Long productId;
	private int quantity;
	private double amount;

	private String failAt;
}
