package com.stock.ms.controller;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.stock.ms.entity.InventoryStockRepository;
import com.stock.ms.entity.Stock;

@RestController
@RequestMapping("/api/2pc/stock")
public class Stock2PCController {

	private static final Logger log = LoggerFactory.getLogger(Stock2PCController.class);

	private final InventoryStockRepository stockRepository;
	// In a real system, pending changes would be in a DB table with a status,
	// but for this demo comparison, we'll use an in-memory map to represent the 'Prepare' state.
	private final Map<Long, Integer> pendingReservations = new ConcurrentHashMap<>();

	public Stock2PCController(InventoryStockRepository stockRepository) {
		this.stockRepository = stockRepository;
	}

	@PostMapping("/prepare")
	public ResponseEntity<String> prepare(@RequestBody Map<String, Object> request) {
		Long orderId = Long.valueOf(request.get("orderId").toString());
		Long productId = Long.valueOf(request.get("productId").toString());
		Integer quantity = Integer.valueOf(request.get("quantity").toString());

		log.info("2PC PREPARE | orderId={} productId={} quantity={}", orderId, productId, quantity);

		Stock stock = stockRepository.findByProductId(productId).orElse(null);
		if (stock == null || stock.getAvailableQuantity() < quantity) {
			log.warn("2PC PREPARE REJECTED | orderId={} reason=insufficient_stock", orderId);
			return ResponseEntity.status(409).body("Insufficient stock");
		}

		// Reserve stock in 'Prepare' phase
		stock.setAvailableQuantity(stock.getAvailableQuantity() - quantity);
		stockRepository.save(stock);
		pendingReservations.put(orderId, quantity);

		return ResponseEntity.ok("PREPARED");
	}

	@PostMapping("/commit")
	public ResponseEntity<String> commit(@RequestBody Map<String, Object> request) {
		Long orderId = Long.valueOf(request.get("orderId").toString());
		log.info("2PC COMMIT | orderId={}", orderId);
		
		if (!pendingReservations.containsKey(orderId)) {
			return ResponseEntity.notFound().build();
		}

		pendingReservations.remove(orderId);
		return ResponseEntity.ok("COMMITTED");
	}

	@PostMapping("/rollback")
	public ResponseEntity<String> rollback(@RequestBody Map<String, Object> request) {
		Long orderId = Long.valueOf(request.get("orderId").toString());
		Long productId = Long.valueOf(request.get("productId").toString());
		log.info("2PC ROLLBACK | orderId={}", orderId);

		Integer reservedQty = pendingReservations.remove(orderId);
		if (reservedQty != null) {
			stockRepository.findByProductId(productId).ifPresent(stock -> {
				stock.setAvailableQuantity(stock.getAvailableQuantity() + reservedQty);
				stockRepository.save(stock);
			});
		}

		return ResponseEntity.ok("ROLLED_BACK");
	}
}
