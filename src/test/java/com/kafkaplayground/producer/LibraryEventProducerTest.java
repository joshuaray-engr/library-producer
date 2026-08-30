package com.kafkaplayground.producer;

import com.kafkaplayground.domain.Book;
import com.kafkaplayground.domain.LibraryEvent;
import com.kafkaplayground.domain.LibraryEventType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Pure unit tests for {@link LibraryEventProducer} - no Spring context, no
 * network calls. {@link KafkaTemplate} is mocked so both the happy path and
 * failure path can be exercised deterministically and quickly.
 */
@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

	private static final String TOPIC = "library-events-test";

	@Mock
	private KafkaTemplate<Integer, String> kafkaTemplate;

	private final ObjectMapper objectMapper = new ObjectMapper();

	private LibraryEventProducer libraryEventProducer;

	@BeforeEach
	void setUp() {
		libraryEventProducer = new LibraryEventProducer(kafkaTemplate, objectMapper);
		ReflectionTestUtils.setField(libraryEventProducer, "topic", TOPIC);
	}

	private LibraryEvent sampleEvent() {
		LibraryEvent event = new LibraryEvent();
		event.setLibraryEventId(1);
		event.setLibraryEventType(LibraryEventType.ADD);
		event.setBook(new Book(1, "Kafka Fundamentals", "Neha Narkhede"));
		return event;
	}

	@Test
	void sendLibraryEvent_publishesSuccessfully() throws Exception {
		LibraryEvent event = sampleEvent();
		ProducerRecord<Integer, String> record = new ProducerRecord<>(TOPIC, event.getLibraryEventId(),
				objectMapper.writeValueAsString(event));
		RecordMetadata metadata = new RecordMetadata(new TopicPartition(TOPIC, 0), 0L, 0, 0L, 0, 0);
		SendResult<Integer, String> sendResult = new SendResult<>(record, metadata);

		when(kafkaTemplate.send(anyString(), anyInt(), anyString()))
				.thenReturn(CompletableFuture.completedFuture(sendResult));

		CompletableFuture<SendResult<Integer, String>> future = libraryEventProducer.sendLibraryEvent(event);

		SendResult<Integer, String> result = future.get();
		assertThat(result.getRecordMetadata().partition()).isZero();
		assertThat(result.getProducerRecord().value()).contains("Kafka Fundamentals");
	}

	@Test
	void sendLibraryEvent_whenKafkaFails_completesExceptionally() {
		LibraryEvent event = sampleEvent();
		CompletableFuture<SendResult<Integer, String>> failedFuture = new CompletableFuture<>();
		failedFuture.completeExceptionally(new RuntimeException("Kafka broker unavailable"));

		when(kafkaTemplate.send(anyString(), anyInt(), anyString())).thenReturn(failedFuture);

		CompletableFuture<SendResult<Integer, String>> future = libraryEventProducer.sendLibraryEvent(event);

		assertThat(future.isCompletedExceptionally()).isTrue();
		assertThrows(ExecutionException.class, future::get);
	}

	@Test
	void sendLibraryEventSynchronous_returnsSendResult() throws Exception {
		LibraryEvent event = sampleEvent();
		ProducerRecord<Integer, String> record = new ProducerRecord<>(TOPIC, event.getLibraryEventId(),
				objectMapper.writeValueAsString(event));
		RecordMetadata metadata = new RecordMetadata(new TopicPartition(TOPIC, 2), 5L, 0, 0L, 0, 0);
		SendResult<Integer, String> sendResult = new SendResult<>(record, metadata);

		when(kafkaTemplate.send(any(ProducerRecord.class)))
				.thenReturn(CompletableFuture.completedFuture(sendResult));

		SendResult<Integer, String> result = libraryEventProducer.sendLibraryEventSynchronous(event);

		assertThat(result.getRecordMetadata().partition()).isEqualTo(2);
		assertThat(result.getRecordMetadata().offset()).isEqualTo(5);
	}

	@Test
	void sendLibraryEventSynchronous_whenKafkaFails_throwsRuntimeException() {
		LibraryEvent event = sampleEvent();
		CompletableFuture<SendResult<Integer, String>> failedFuture = new CompletableFuture<>();
		failedFuture.completeExceptionally(new RuntimeException("boom"));

		when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture);

		assertThrows(RuntimeException.class, () -> libraryEventProducer.sendLibraryEventSynchronous(event));
	}

}

