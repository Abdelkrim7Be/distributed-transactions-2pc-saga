package com.order.ms.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.sc.saga.SagaEvent;
import com.sc.saga.SagaKafkaTopicFactory;

import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

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
	public KafkaTemplate<String, SagaEvent> sagaEventKafkaTemplate(
			@Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
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

	@Bean
	public DefaultErrorHandler defaultErrorHandler(KafkaTemplate<String, ?> template) {
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
				(record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));
		// Retry 3 times with 1s intervals
		return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
	}
}
