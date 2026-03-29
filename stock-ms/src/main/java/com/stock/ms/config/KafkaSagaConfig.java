package com.stock.ms.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.sc.saga.SagaEvent;
import com.sc.saga.SagaKafkaTopicFactory;

@Configuration
public class KafkaSagaConfig {

	@Bean
	public NewTopic sagaOrderEventsTopic() {
		return SagaKafkaTopicFactory.orderEventsTopic();
	}

	@Bean
	public NewTopic sagaStockEventsTopic() {
		return SagaKafkaTopicFactory.stockEventsTopic();
	}

	@Bean
	public NewTopic sagaPaymentEventsTopic() {
		return SagaKafkaTopicFactory.paymentEventsTopic();
	}

	@Bean
	public NewTopic sagaDeliveryEventsTopic() {
		return SagaKafkaTopicFactory.deliveryEventsTopic();
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, SagaEvent> sagaEventKafkaListenerContainerFactory(
			@Value("${spring.kafka.bootstrap-servers}") String bootstrap,
			@Value("${spring.kafka.consumer.group-id}") String groupId) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		JsonDeserializer<SagaEvent> deserializer = new JsonDeserializer<>(SagaEvent.class, false);
		deserializer.addTrustedPackages("com.sc.saga");
		ConsumerFactory<String, SagaEvent> consumerFactory =
				new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
		ConcurrentKafkaListenerContainerFactory<String, SagaEvent> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		return factory;
	}
}
