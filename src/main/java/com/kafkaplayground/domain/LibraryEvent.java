package com.kafkaplayground.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The event that gets published to Kafka whenever a Book is added or updated.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LibraryEvent {

	private Integer libraryEventId;

	private LibraryEventType libraryEventType;

	@NotNull(message = "Book cannot be null")
	@Valid
	private Book book;

}

