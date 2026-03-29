package com.stock.ms.entity;

import java.util.Optional;

import org.springframework.data.repository.CrudRepository;

public interface InventoryStockRepository extends CrudRepository<Stock, Long> {

	Optional<Stock> findByProductId(Long productId);
}
