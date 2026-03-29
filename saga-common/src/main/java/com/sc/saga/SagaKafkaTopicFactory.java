package com.sc.saga;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Builds {@link NewTopic} beans for local / single-broker clusters (replication factor 1).
 */
public final class SagaKafkaTopicFactory {

	private static final int PARTITIONS = 3;
	private static final short REPLICAS = 1;

	private SagaKafkaTopicFactory() {
	}

	public static NewTopic orderEventsTopic() {
		return TopicBuilder.name(SagaTopics.ORDER_EVENTS).partitions(PARTITIONS).replicas(REPLICAS).build();
	}

	public static NewTopic stockEventsTopic() {
		return TopicBuilder.name(SagaTopics.STOCK_EVENTS).partitions(PARTITIONS).replicas(REPLICAS).build();
	}

	public static NewTopic paymentEventsTopic() {
		return TopicBuilder.name(SagaTopics.PAYMENT_EVENTS).partitions(PARTITIONS).replicas(REPLICAS).build();
	}

	public static NewTopic deliveryEventsTopic() {
		return TopicBuilder.name(SagaTopics.DELIVERY_EVENTS).partitions(PARTITIONS).replicas(REPLICAS).build();
	}
}
