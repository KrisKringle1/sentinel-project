package com.sentinel.consumer.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.sentinel.common.avro.SentinelMetric;
import com.sentinel.consumer.dto.DlqMetricRecord;
import com.sentinel.consumer.dto.TelemetryStreamEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricConsumerServiceTest {

  private TelemetryStateService telemetryStateService;
  private SseEmitterService sseEmitterService;
  private MetricConsumerService metricConsumerService;

  @BeforeEach
  void setUp() {
    telemetryStateService = mock(TelemetryStateService.class);
    sseEmitterService = mock(SseEmitterService.class);
    metricConsumerService = new MetricConsumerService(telemetryStateService, sseEmitterService);
  }

  @Test
  void shouldRecordAndBroadcastConsumedMetric() {
    SentinelMetric metric =
        SentinelMetric.newBuilder()
            .setServiceId("billing-service")
            .setMetricName("latency_ms")
            .setValue(180.0)
            .setTimestamp(1000L)
            .build();

    TelemetryStreamEvent streamEvent =
        TelemetryStreamEvent.builder()
            .eventType("METRIC")
            .serviceId("billing-service")
            .metricName("latency_ms")
            .value(180.0)
            .timestamp(1000L)
            .build();

    when(telemetryStateService.recordMetric(metric)).thenReturn(streamEvent);

    metricConsumerService.consumeMetric(metric);

    verify(telemetryStateService).recordMetric(metric);
    verify(sseEmitterService).broadcast(streamEvent);
  }

  @Test
  void shouldIgnoreNullMetricPayload() {
    metricConsumerService.consumeMetric(null);
    verifyNoInteractions(telemetryStateService, sseEmitterService);
  }

  @Test
  void shouldRecordDlqMetric() {
    SentinelMetric dlqMetric =
        SentinelMetric.newBuilder()
            .setServiceId("billing-service")
            .setMetricName("latency_ms")
            .setValue(999.0)
            .setTimestamp(2000L)
            .build();

    when(telemetryStateService.recordDlqMetric(dlqMetric))
        .thenReturn(
            DlqMetricRecord.builder()
                .serviceId("billing-service")
                .metricName("latency_ms")
                .value(999.0)
                .metricTimestamp(2000L)
                .receivedAtTimestamp(2001L)
                .build());

    metricConsumerService.consumeDlqMetric(dlqMetric);

    verify(telemetryStateService).recordDlqMetric(dlqMetric);
  }
}
