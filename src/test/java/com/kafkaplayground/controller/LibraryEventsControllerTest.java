package com.kafkaplayground.controller;

import com.kafkaplayground.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Slice test for {@link LibraryEventsController}. The producer is mocked so
 * no Kafka broker (real or embedded) is needed; {@link LibraryEventsControllerAdvice}
 * is picked up automatically by {@code @WebMvcTest}.
 */
@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private LibraryEventProducer libraryEventProducer;

	@Test
	void postLibraryEvent_withValidPayload_returns201() throws Exception {
		when(libraryEventProducer.sendLibraryEvent(any())).thenReturn(CompletableFuture.completedFuture(null));

		String payload = """
				{
				  "book": {
				    "bookId": 1,
				    "bookName": "Kafka Fundamentals",
				    "bookAuthor": "Neha Narkhede"
				  }
				}
				""";

		mockMvc.perform(post("/v1/libraryevent")
						.contentType(MediaType.APPLICATION_JSON)
						.content(payload))
				.andExpect(status().isCreated())
				.andExpect(jsonPath("$.libraryEventType").value("ADD"))
				.andExpect(jsonPath("$.book.bookName").value("Kafka Fundamentals"));

		verify(libraryEventProducer).sendLibraryEvent(any());
	}

	@Test
	void postLibraryEvent_withBlankBookName_returns400WithFieldError() throws Exception {
		String payload = """
				{
				  "book": {
				    "bookId": 1,
				    "bookName": "",
				    "bookAuthor": "Neha Narkhede"
				  }
				}
				""";

		mockMvc.perform(post("/v1/libraryevent")
						.contentType(MediaType.APPLICATION_JSON)
						.content(payload))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$['book.bookName']").value("Book Name cannot be blank"));

		verifyNoInteractions(libraryEventProducer);
	}

	@Test
	void postLibraryEvent_withMissingBook_returns400() throws Exception {
		mockMvc.perform(post("/v1/libraryevent")
						.contentType(MediaType.APPLICATION_JSON)
						.content("{}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.book").value("Book cannot be null"));

		verifyNoInteractions(libraryEventProducer);
	}

	@Test
	void putLibraryEvent_withValidPayload_returns200() throws Exception {
		when(libraryEventProducer.sendLibraryEvent(any())).thenReturn(CompletableFuture.completedFuture(null));

		String payload = """
				{
				  "libraryEventId": 1,
				  "book": {
				    "bookId": 1,
				    "bookName": "Kafka Fundamentals 2nd Ed",
				    "bookAuthor": "Neha Narkhede"
				  }
				}
				""";

		mockMvc.perform(put("/v1/libraryevent")
						.contentType(MediaType.APPLICATION_JSON)
						.content(payload))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.libraryEventType").value("UPDATE"));

		verify(libraryEventProducer).sendLibraryEvent(any());
	}

	@Test
	void putLibraryEvent_withoutLibraryEventId_returns400() throws Exception {
		String payload = """
				{
				  "book": {
				    "bookId": 1,
				    "bookName": "X",
				    "bookAuthor": "Y"
				  }
				}
				""";

		mockMvc.perform(put("/v1/libraryevent")
						.contentType(MediaType.APPLICATION_JSON)
						.content(payload))
				.andExpect(status().isBadRequest())
				.andExpect(content().string("Please pass the LibraryEventId"));

		verifyNoInteractions(libraryEventProducer);
	}

	@Test
	void putLibraryEvent_withBlankBookAuthor_returns400WithFieldError() throws Exception {
		String payload = """
				{
				  "libraryEventId": 1,
				  "book": {
				    "bookId": 1,
				    "bookName": "X",
				    "bookAuthor": ""
				  }
				}
				""";

		mockMvc.perform(put("/v1/libraryevent")
						.contentType(MediaType.APPLICATION_JSON)
						.content(payload))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$['book.bookAuthor']").value("Book Author cannot be blank"));

		verifyNoInteractions(libraryEventProducer);
	}

}




