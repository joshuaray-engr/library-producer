package com.kafkaplayground.controller;

import com.kafkaplayground.domain.LibraryEvent;
import com.kafkaplayground.domain.LibraryEventType;
import com.kafkaplayground.producer.LibraryEventProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Exposes REST endpoints to publish {@link LibraryEvent}s to Kafka.
 */
@RestController
@RequestMapping("/v1")
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsController {

	private final LibraryEventProducer libraryEventProducer;

	@PostMapping("/libraryevent")
	public ResponseEntity<?> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {

		libraryEvent.setLibraryEventType(LibraryEventType.ADD);
		libraryEventProducer.sendLibraryEvent(libraryEvent);

		log.info("Library Event : {}", libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}

	@PutMapping("/libraryevent")
	public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {

		if (libraryEvent.getLibraryEventId() == null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body("Please pass the LibraryEventId");
		}

		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEvent(libraryEvent);

		log.info("Library Event : {}", libraryEvent);
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}

}



