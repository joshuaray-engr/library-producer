package com.kafkaplayground;

import com.kafkaplayground.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full end-to-end test: real Spring context, real HTTP call through
 * {@link TestRestTemplate}, and a real (embedded) Kafka broker, asserting the
 * message that lands on the topic. No dependency on the remote AWS cluster.
 * <p>
 * Both tests share the same embedded topic/broker (Spring context caching),
 * so instead of asserting "exactly one record", each test polls all
 * currently-available records and asserts that one of them matches its own
 * uniquely-identifiable payload.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@EmbeddedKafka(topics = "library-events-test", partitions = 1)
@ActiveProfiles("test")
class LibraryEventsIntegrationTest {

	private static final String TOPIC = "library-events-test";

	@Autowired
	private TestRestTemplate restTemplate;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	private Consumer<Integer, String> consumer;

	@BeforeEach
	void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafkaBroker,
				"library-events-test-group", true);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		DefaultKafkaConsumerFactory<Integer, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = consumerFactory.createConsumer();
		embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, TOPIC);
	}

	@AfterEach
	void tearDown() {
		consumer.close();
	}

	@Test
	void postLibraryEvent_publishesMessageToKafkaTopic() {
		String payload = """
				{
				  "book": {
				    "bookId": 456,
				    "bookName": "Kafka Streams in Action",
				    "bookAuthor": "Bill Bejeck"
				  }
				}
				""";

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<String> request = new HttpEntity<>(payload, headers);

		ResponseEntity<LibraryEvent> response = restTemplate.postForEntity("/v1/libraryevent", request,
				LibraryEvent.class);

		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
		assertThat(response.getBody()).isNotNull();
		assertThat(response.getBody().getLibraryEventType().name()).isEqualTo("ADD");

		ConsumerRecord<Integer, String> record = findRecordContaining("Kafka Streams in Action\"");
		assertThat(record.value()).contains("\"libraryEventType\":\"ADD\"");
	}

	@Test
	void putLibraryEvent_publishesUpdateMessageKeyedByLibraryEventId() {
		String payload = """
				{
				  "libraryEventId": 42,
				  "book": {
				    "bookId": 456,
				    "bookName": "Kafka Streams in Action 2nd Ed",
				    "bookAuthor": "Bill Bejeck"
				  }
				}
				""";

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<String> request = new HttpEntity<>(payload, headers);

		ResponseEntity<LibraryEvent> response = restTemplate.exchange("/v1/libraryevent",
				org.springframework.http.HttpMethod.PUT, request, LibraryEvent.class);

		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

		ConsumerRecord<Integer, String> record = findRecordContaining("\"libraryEventId\":42");
		assertThat(record.key()).isEqualTo(42);
		assertThat(record.value()).contains("\"libraryEventType\":\"UPDATE\"");
	}

	/**
	 * Polls all currently-available records on the test topic (across
	 * multiple polls, since embedded-broker delivery can be split across
	 * polls) and returns the first one whose value contains the given marker.
	 * Both tests in this class share the same embedded topic/context, so we
	 * can't assert "exactly one record" - we look for our own message.
	 */
	private ConsumerRecord<Integer, String> findRecordContaining(String marker) {
		long deadline = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
		while (System.currentTimeMillis() < deadline) {
			ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(500));
			Optional<ConsumerRecord<Integer, String>> match = StreamSupport
					.stream(records.spliterator(), false)
					.filter(r -> r.value().contains(marker))
					.findFirst();
			if (match.isPresent()) {
				return match.get();
			}
		}
		throw new AssertionError("No record found on topic " + TOPIC + " containing marker: " + marker);
	}

}










