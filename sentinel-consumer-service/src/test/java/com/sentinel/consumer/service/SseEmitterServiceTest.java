package com.sentinel.consumer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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
  void shouldTrackMultipleClientsAndHandleBurstBroadcastWithoutThrowing() {
    SseEmitter firstEmitter = sseEmitterService.createEmitter();
    SseEmitter secondEmitter = sseEmitterService.createEmitter();

    assertThat(firstEmitter).isNotNull();
    assertThat(secondEmitter).isNotNull();
    assertThat(sseEmitterService.getActiveClientCount()).isEqualTo(2);

    // Broadcast a burst exceeding MAX_PENDING_EVENTS_PER_CLIENT (100) to verify bounded queue
    // safety
    assertThatCode(
            () -> {
              for (int i = 0; i < 125; i++) {
                sseEmitterService.broadcast(buildSampleEvent(i));
              }
            })
        .doesNotThrowAnyException();
  }

  @Test
  void shouldNoOpGracefullyWhenBroadcastingWithZeroClients() {
    assertThat(sseEmitterService.getActiveClientCount()).isZero();
    assertThatCode(() -> sseEmitterService.broadcast(buildSampleEvent(1)))
        .doesNotThrowAnyException();
    assertThat(sseEmitterService.getActiveClientCount()).isZero();
  }

  private static TelemetryStreamEvent buildSampleEvent(int seq) {
    return TelemetryStreamEvent.builder()
        .eventType("ALERT")
        .serviceId("auth-service")
        .metricName("cpu_usage")
        .value(96.0)
        .timestamp(1000L + seq)
        .alert(
            AnomalyAlert.builder()
                .alertId("a-" + seq)
                .serviceId("auth-service")
                .metricName("cpu_usage")
                .value(96.0)
                .threshold(90.0)
                .severity("CRITICAL")
                .message("CPU high")
                .timestamp(1000L + seq)
                .build())
        .build();
  }
}
