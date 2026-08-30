package com.kafkaplayground.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Represents a Book that is part of a {@link LibraryEvent}.
 */
public record Book(

		@NotNull(message = "Book Id cannot be null")
		Integer bookId,

		@NotBlank(message = "Book Name cannot be blank")
		String bookName,

		@NotBlank(message = "Book Author cannot be blank")
		String bookAuthor
) {
}

