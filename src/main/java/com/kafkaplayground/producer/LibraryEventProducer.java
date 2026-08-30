package com.kafkaplayground.producer;

import com.kafkaplayground.domain.LibraryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes {@link LibraryEvent}s to the configured Kafka topic.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class LibraryEventProducer {

	private final KafkaTemplate<Integer, String> kafkaTemplate;

	private final ObjectMapper objectMapper;

	@Value("${library-events.topic}")
	private String topic;

	/**
	 * Publishes the {@link LibraryEvent} asynchronously and logs success/failure.
	 */
	public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

		CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(topic, key, value);

		completableFuture.whenComplete((result, throwable) -> {
			if (throwable != null) {
				handleFailure(key, value, throwable);
			} else {
				handleSuccess(key, value, result);
			}
		});

		return completableFuture;
	}

	/**
	 * Publishes the {@link LibraryEvent} using a {@link ProducerRecord} so custom headers can be attached,
	 * and blocks until the send completes (or the default send timeout elapses).
	 */
	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

		try {
			return kafkaTemplate.send(buildProducerRecord(key, value)).get();
		} catch (Exception e) {
			log.error("Exception sending the message and the exception is {}", e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
		return new ProducerRecord<>(topic, null, key, value, null);
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message sent successfully for the key : {} and the value is {}, partition is {}",
				key, value, result.getRecordMetadata().partition());
	}

	private void handleFailure(Integer key, String value, Throwable throwable) {
		log.error("Error sending the message and the exception is {}", throwable.getMessage(), throwable);
	}

}



