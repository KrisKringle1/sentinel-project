package com.sentinel.ingestion.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.ingestion.dto.MetricRequest;
import com.sentinel.ingestion.exception.MetricProcessingException;
import com.sentinel.ingestion.service.MetricProducerService;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(MetricsController.class)
class MetricsControllerTest {

  @Autowired private MockMvc mockMvc;

  @Autowired private ObjectMapper objectMapper;

  @MockBean private MetricProducerService metricProducerService;

  @Test
  void shouldAcceptValidMetricRequest() throws Exception {
    MetricRequest request =
        MetricRequest.builder()
            .serviceId("auth-service")
            .metricName("cpu_usage")
            .value(65.2)
            .unit("percent")
            .timestamp(1700000000000L)
            .build();

    when(metricProducerService.sendMetric(any(MetricRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    mockMvc
        .perform(
            post("/api/v1/metrics")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
        .andExpect(status().isAccepted())
        .andExpect(content().string("Metric received"));

    verify(metricProducerService).sendMetric(request);
  }

  @Test
  void shouldReturnBadRequestWhenRequiredFieldsAreInvalid() throws Exception {
    String invalidPayload =
        """
        {
          "serviceId": "",
          "metricName": "cpu_usage",
          "value": null,
          "timestamp": 1700000000000
        }
        """;

    mockMvc
        .perform(
            post("/api/v1/metrics").contentType(MediaType.APPLICATION_JSON).content(invalidPayload))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.message").value("Validation failed"))
        .andExpect(jsonPath("$.details", hasItem("serviceId: serviceId cannot be empty")))
        .andExpect(jsonPath("$.details", hasItem("value: value cannot be null")));

    verifyNoInteractions(metricProducerService);
  }

  @Test
  void shouldReturnInternalServerErrorWhenMetricProcessingFails() throws Exception {
    MetricRequest request =
        MetricRequest.builder()
            .serviceId("auth-service")
            .metricName("cpu_usage")
            .value(91.0)
            .unit("percent")
            .timestamp(1700000000000L)
            .build();

    doThrow(new MetricProcessingException("Kafka serialization error"))
        .when(metricProducerService)
        .sendMetric(any(MetricRequest.class));

    mockMvc
        .perform(
            post("/api/v1/metrics")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.status").value(500))
        .andExpect(
            jsonPath(
                "$.message",
                containsString("An internal error occurred while processing the metric.")));
  }
}
