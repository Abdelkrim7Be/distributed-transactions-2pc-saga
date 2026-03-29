package com.order.ms.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "orders")
public class Order {

	@Id
	@GeneratedValue
	private Long id;

	@Column(nullable = false)
	private Long productId;

	@Column(nullable = false)
	private int quantity;

	@Column(nullable = false)
	private double amount;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	private OrderStatus status;

	public Map<String, Object> toSagaPayload() {
		Map<String, Object> m = new HashMap<>();
		m.put("productId", productId);
		m.put("quantity", quantity);
		m.put("amount", amount);
		return m;
	}
}
