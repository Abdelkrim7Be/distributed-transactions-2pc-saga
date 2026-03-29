package com.stock.ms.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.stock.ms.dto.Stock;
import com.stock.ms.entity.WareHouse;
import com.stock.ms.entity.StockRepository;

@RestController
@RequestMapping("/api")
public class StockController {

	@Autowired
	private StockRepository repository;

	@PostMapping("/addItems")
	public void addItems(@RequestBody Stock stock) {
		Iterable<WareHouse> items = repository.findByItem(stock.getItem());

		if (items.iterator().hasNext()) {
			items.forEach(i -> {
				i.setQuantity(stock.getQuantity() + i.getQuantity());
				repository.save(i);
			});
		} else {
			WareHouse i = new WareHouse();
			i.setItem(stock.getItem());
			i.setQuantity(stock.getQuantity());
			repository.save(i);
		}
	}
}
