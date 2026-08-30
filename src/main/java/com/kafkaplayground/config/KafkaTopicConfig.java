package com.kafkaplayground.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Explicitly provisions the library-events Kafka topic on application startup,
 * instead of relying on the broker's {@code auto.create.topics.enable} setting
 * (which is often disabled in production clusters).
 * <p>
 * Spring Boot auto-configures a {@code KafkaAdmin} bean using
 * {@code spring.kafka.bootstrap-servers}; any {@link NewTopic} bean defined
 * here is created idempotently at startup (a no-op if the topic already exists,
 * including its configs - existing topics are NOT retroactively altered).
 */
@Configuration
public class KafkaTopicConfig {

	@Value("${library-events.topic}")
	private String topicName;

	@Value("${library-events.topic.partitions:3}")
	private int partitions;

	@Value("${library-events.topic.replicas:3}")
	private int replicas;

	@Value("${library-events.topic.min-insync-replicas:2}")
	private String minInSyncReplicas;

	@Bean
	public NewTopic libraryEventsTopic() {
		return TopicBuilder.name(topicName)
				.partitions(partitions)
				.replicas(replicas)
				// With replicas=3 and producer acks=all, requiring 2 in-sync
				// replicas means a write is durable even if 1 broker is down,
				// while still rejecting writes if 2+ brokers are unavailable.
				.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSyncReplicas)
				.build();
	}

}

