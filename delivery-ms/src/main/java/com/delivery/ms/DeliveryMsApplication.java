package com.delivery.ms;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class DeliveryMsApplication {

	public static void main(String[] args) {
		SpringApplication.run(DeliveryMsApplication.class, args);
	}

}
