package com.sentinel.consumer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.sentinel.consumer.dto.AnomalyAlert;
import com.sentinel.consumer.dto.TelemetryStreamEvent;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    if (sseEmitterService != null) {
      sseEmitterService.shutdown();
    }
  }

  @Test
  void shouldTrackMultipleClientsAndDeliverBurstBroadcastAsynchronously() throws Exception {
    int totalEvents = 125;
    CountDownLatch firstClientLatch = new CountDownLatch(1);
    CountDownLatch secondClientLatch = new CountDownLatch(1);
    AtomicInteger deliveredCount = new AtomicInteger(0);

    SseEmitter firstEmitter =
        sseEmitterService.registerEmitter(
            new SseEmitter(0L) {
              @Override
              public void send(SseEventBuilder builder) throws IOException {
                if (deliveredCount.incrementAndGet() >= 2) {
                  firstClientLatch.countDown();
                }
              }
            });

    SseEmitter secondEmitter =
        sseEmitterService.registerEmitter(
            new SseEmitter(0L) {
              @Override
              public void send(SseEventBuilder builder) throws IOException {
                secondClientLatch.countDown();
              }
            });

    assertThat(firstEmitter).isNotNull();
    assertThat(secondEmitter).isNotNull();
    assertThat(sseEmitterService.getActiveClientCount()).isEqualTo(2);

    // Broadcast a burst exceeding MAX_PENDING_EVENTS_PER_CLIENT (100) to verify bounded queue
    // safety
    for (int i = 0; i < totalEvents; i++) {
      sseEmitterService.broadcast(buildSampleEvent(i));
    }

    assertThat(firstClientLatch.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(secondClientLatch.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(deliveredCount.get()).isGreaterThanOrEqualTo(2);
  }

  @Test
  void shouldRemoveClientWhenSendThrowsException() throws Exception {
    CountDownLatch disconnectLatch = new CountDownLatch(1);
    AtomicInteger sendAttempts = new AtomicInteger(0);

    sseEmitterService.registerEmitter(
        new SseEmitter(0L) {
          @Override
          public void send(SseEventBuilder builder) throws IOException {
            // Allow the initial "connected" event (attempt 1), then fail on broadcast (attempt 2)
            if (sendAttempts.incrementAndGet() >= 2) {
              throw new IOException("Simulated client disconnect");
            }
          }

          @Override
          public synchronized void complete() {
            super.complete();
            disconnectLatch.countDown();
          }
        });

    assertThat(sseEmitterService.getActiveClientCount()).isEqualTo(1);

    sseEmitterService.broadcast(buildSampleEvent(1));

    assertThat(disconnectLatch.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(sseEmitterService.getActiveClientCount()).isZero();
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
