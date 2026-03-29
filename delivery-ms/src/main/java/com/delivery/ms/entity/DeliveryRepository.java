package com.delivery.ms.entity;

import java.util.Optional;

import org.springframework.data.repository.CrudRepository;

public interface DeliveryRepository extends CrudRepository<Delivery, Long> {

	Optional<Delivery> findFirstByOrderId(long orderId);
}
