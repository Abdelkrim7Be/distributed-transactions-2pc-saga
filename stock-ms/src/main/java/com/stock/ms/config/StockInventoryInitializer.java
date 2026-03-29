package com.stock.ms.config;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.stock.ms.entity.InventoryStockRepository;
import com.stock.ms.entity.Stock;

@Component
public class StockInventoryInitializer implements ApplicationRunner {

	private final InventoryStockRepository inventoryStockRepository;

	public StockInventoryInitializer(InventoryStockRepository inventoryStockRepository) {
		this.inventoryStockRepository = inventoryStockRepository;
	}

	@Override
	public void run(ApplicationArguments args) {
		if (inventoryStockRepository.count() > 0) {
			return;
		}
		Stock row = new Stock();
		row.setProductId(1L);
		row.setAvailableQuantity(100);
		inventoryStockRepository.save(row);
	}
}
