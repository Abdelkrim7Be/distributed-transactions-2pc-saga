package com.order.ms.dto;

import com.order.ms.entity.OrderStatus;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class OrderResponse {

	private final Long id;
	private final OrderStatus status;
}
