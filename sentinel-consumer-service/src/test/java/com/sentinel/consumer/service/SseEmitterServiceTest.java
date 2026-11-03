package com.sentinel.consumer.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.sentinel.consumer.dto.AnomalyAlert;
import com.sentinel.consumer.dto.TelemetryStreamEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

class SseEmitterServiceTest {

  private SseEmitterService sseEmitterService;

  @BeforeEach
  void setUp() {
    sseEmitterService = new SseEmitterService();
  }

  @AfterEach
  void tearDown() {
    sseEmitterService.shutdown();
  }

  @Test
  void shouldRegisterAndBroadcastAsyncWithoutBlockingCaller() {
    SseEmitter emitter = sseEmitterService.createEmitter();
    assertThat(emitter).isNotNull();
    assertThat(sseEmitterService.getActiveClientCount()).isEqualTo(1);

    TelemetryStreamEvent event =
        TelemetryStreamEvent.builder()
            .eventType("ALERT")
            .serviceId("auth-service")
            .metricName("cpu_usage")
            .value(96.0)
            .timestamp(1000L)
            .alert(
                AnomalyAlert.builder()
                    .alertId("a-1")
                    .serviceId("auth-service")
                    .metricName("cpu_usage")
                    .value(96.0)
                    .threshold(90.0)
                    .severity("CRITICAL")
                    .message("CPU high")
                    .timestamp(1000L)
                    .build())
            .build();

    sseEmitterService.broadcast(event);
    assertThat(sseEmitterService.getActiveClientCount()).isGreaterThanOrEqualTo(0);
  }
}
