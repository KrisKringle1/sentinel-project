package com.sentinel.consumer.controller;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.sentinel.consumer.dto.AnomalyAlert;
import com.sentinel.consumer.dto.DlqMetricRecord;
import com.sentinel.consumer.dto.MetricSummary;
import com.sentinel.consumer.dto.ServiceTelemetrySummary;
import com.sentinel.consumer.service.SseEmitterService;
import com.sentinel.consumer.service.TelemetryStateService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@WebMvcTest(TelemetryController.class)
class TelemetryControllerTest {

  @Autowired private MockMvc mockMvc;

  @MockBean private TelemetryStateService telemetryStateService;

  @MockBean private SseEmitterService sseEmitterService;

  @Test
  void shouldReturnServiceSummaries() throws Exception {
    ServiceTelemetrySummary summary =
        ServiceTelemetrySummary.builder()
            .serviceId("api-gateway")
            .status("HEALTHY")
            .totalEvents(12)
            .anomalyCount(0)
            .lastSeenTimestamp(1700000000000L)
            .metrics(
                Map.of(
                    "cpu_usage",
                    MetricSummary.builder()
                        .metricName("cpu_usage")
                        .count(12)
                        .min(18.0)
                        .max(64.0)
                        .movingAverage(41.5)
                        .lastValue(39.0)
                        .lastUpdated(1700000000000L)
                        .build()))
            .build();

    when(telemetryStateService.getAllServices()).thenReturn(List.of(summary));

    mockMvc
        .perform(get("/api/v1/telemetry/services"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].serviceId").value("api-gateway"))
        .andExpect(jsonPath("$[0].status").value("HEALTHY"))
        .andExpect(jsonPath("$[0].metrics.cpu_usage.movingAverage").value(41.5));
  }

  @Test
  void shouldReturnServiceByIdOrNotFound() throws Exception {
    ServiceTelemetrySummary summary =
        ServiceTelemetrySummary.builder()
            .serviceId("api-gateway")
            .status("HEALTHY")
            .totalEvents(5)
            .anomalyCount(0)
            .lastSeenTimestamp(1700000000000L)
            .metrics(Map.of())
            .build();

    when(telemetryStateService.getService("api-gateway")).thenReturn(Optional.of(summary));
    when(telemetryStateService.getService("unknown")).thenReturn(Optional.empty());

    mockMvc
        .perform(get("/api/v1/telemetry/services/api-gateway"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.serviceId").value("api-gateway"));

    mockMvc.perform(get("/api/v1/telemetry/services/unknown")).andExpect(status().isNotFound());
  }

  @Test
  void shouldReturnAlertsFilteredByServiceAndLimit() throws Exception {
    AnomalyAlert alert =
        AnomalyAlert.builder()
            .alertId("alert-1")
            .serviceId("auth-service")
            .metricName("cpu_usage")
            .value(95.5)
            .threshold(90.0)
            .severity("CRITICAL")
            .message("Service 'auth-service' reported cpu_usage=95.50 (threshold: 90.00)")
            .timestamp(1700000000000L)
            .build();

    when(telemetryStateService.getAlerts("auth-service", 10)).thenReturn(List.of(alert));

    mockMvc
        .perform(
            get("/api/v1/telemetry/alerts").param("serviceId", "auth-service").param("limit", "10"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].alertId").value("alert-1"))
        .andExpect(jsonPath("$[0].severity").value("CRITICAL"));
  }

  @Test
  void shouldReturnDlqRecords() throws Exception {
    DlqMetricRecord dlqRecord =
        DlqMetricRecord.builder()
            .serviceId("search-service")
            .metricName("latency_ms")
            .value(750.0)
            .metricTimestamp(1700000000000L)
            .receivedAtTimestamp(1700000000100L)
            .build();

    when(telemetryStateService.getDlqRecords()).thenReturn(List.of(dlqRecord));

    mockMvc
        .perform(get("/api/v1/telemetry/dlq"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].serviceId").value("search-service"));
  }

  @Test
  void shouldCreateSseStream() throws Exception {
    SseEmitter emitter = new SseEmitter(1000L);
    emitter.complete();
    when(sseEmitterService.createEmitter()).thenReturn(emitter);

    mockMvc
        .perform(get("/api/v1/telemetry/stream").accept(MediaType.TEXT_EVENT_STREAM))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM));
  }
}
