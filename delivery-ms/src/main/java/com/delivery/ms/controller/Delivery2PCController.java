package com.delivery.ms.controller;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.delivery.ms.entity.Delivery;
import com.delivery.ms.entity.DeliveryRepository;

@RestController
@RequestMapping("/api/2pc/delivery")
public class Delivery2PCController {

	private static final Logger log = LoggerFactory.getLogger(Delivery2PCController.class);

	private final DeliveryRepository deliveryRepository;
	private final Map<Long, String> pendingDeliveries = new ConcurrentHashMap<>();

	public Delivery2PCController(DeliveryRepository deliveryRepository) {
		this.deliveryRepository = deliveryRepository;
	}

	@PostMapping("/prepare")
	public ResponseEntity<String> prepare(@RequestBody Map<String, Object> request) {
		Long orderId = Long.valueOf(request.get("orderId").toString());
		String address = request.getOrDefault("address", "default").toString();

		log.info("2PC PREPARE | orderId={} address={}", orderId, address);

		pendingDeliveries.put(orderId, address);
		return ResponseEntity.ok("PREPARED");
	}

	@PostMapping("/commit")
	public ResponseEntity<String> commit(@RequestBody Map<String, Object> request) {
		Long orderId = Long.valueOf(request.get("orderId").toString());
		String address = pendingDeliveries.remove(orderId);

		log.info("2PC COMMIT | orderId={}", orderId);

		if (address != null) {
			Delivery delivery = new Delivery();
			delivery.setOrderId(orderId);
			delivery.setAddress(address);
			delivery.setStatus("scheduled (2PC)");
			deliveryRepository.save(delivery);
		}

		return ResponseEntity.ok("COMMITTED");
	}

	@PostMapping("/rollback")
	public ResponseEntity<String> rollback(@RequestBody Map<String, Object> request) {
		Long orderId = Long.valueOf(request.get("orderId").toString());
		log.info("2PC ROLLBACK | orderId={}", orderId);
		pendingDeliveries.remove(orderId);
		return ResponseEntity.ok("ROLLED_BACK");
	}
}
