package com.delivery.ms.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class DeliveryController {

	@GetMapping
	public String index() {
		return "Delivery Service is running.";
	}
}
