package com.kafkaplayground;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

/**
 * Verifies the Spring context loads successfully. Uses the embedded broker
 * (via the "test" profile) instead of the remote AWS cluster so this test
 * never depends on network access.
 */
@SpringBootTest
@EmbeddedKafka(topics = "library-events-test", partitions = 1)
@ActiveProfiles("test")
class LibraryProducerApplicationTests {

	@Test
	void contextLoads() {
	}

}
