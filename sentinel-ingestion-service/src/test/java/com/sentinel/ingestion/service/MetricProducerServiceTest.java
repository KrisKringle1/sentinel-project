package com.sentinel.ingestion.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sentinel.common.avro.SentinelMetric;
import com.sentinel.ingestion.dto.MetricRequest;
import com.sentinel.ingestion.exception.MetricProcessingException;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class MetricProducerServiceTest {

  private static final String METRICS_TOPIC = "service-metrics";
  private static final String DLQ_TOPIC = "service-metrics-dlq";

  private KafkaTemplate<String, SentinelMetric> kafkaTemplate;
  private MetricProducerService producerService;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    kafkaTemplate = mock(KafkaTemplate.class);
    producerService = new MetricProducerService(kafkaTemplate, METRICS_TOPIC, DLQ_TOPIC);
  }

  @Test
  void shouldSerializeAndSendMetricToPrimaryTopic() {
    MetricRequest request =
        MetricRequest.builder()
            .serviceId("auth-service")
            .metricName("cpu_usage")
            .value(72.5)
            .unit("percent")
            .timestamp(1700000000000L)
            .build();

    SentinelMetric expectedAvro =
        SentinelMetric.newBuilder()
            .setServiceId("auth-service")
            .setMetricName("cpu_usage")
            .setValue(72.5)
            .setTimestamp(1700000000000L)
            .build();

    SendResult<String, SentinelMetric> sendResult =
        new SendResult<>(
            new ProducerRecord<>(METRICS_TOPIC, "auth-service", expectedAvro),
            new RecordMetadata(new TopicPartition(METRICS_TOPIC, 0), 0L, 0, 0L, 0, 0));

    when(kafkaTemplate.send(eq(METRICS_TOPIC), eq("auth-service"), any(SentinelMetric.class)))
        .thenReturn(CompletableFuture.completedFuture(sendResult));

    CompletableFuture<SendResult<String, SentinelMetric>> future =
        producerService.sendMetric(request);

    assertThat(future).isCompletedWithValue(sendResult);

    ArgumentCaptor<SentinelMetric> metricCaptor = ArgumentCaptor.forClass(SentinelMetric.class);
    verify(kafkaTemplate).send(eq(METRICS_TOPIC), eq("auth-service"), metricCaptor.capture());
    verify(kafkaTemplate, never()).send(eq(DLQ_TOPIC), any(), any());

    SentinelMetric captured = metricCaptor.getValue();
    assertThat(captured.getServiceId().toString()).isEqualTo("auth-service");
    assertThat(captured.getMetricName().toString()).isEqualTo("cpu_usage");
    assertThat(captured.getValue()).isEqualTo(72.5);
    assertThat(captured.getTimestamp()).isEqualTo(1700000000000L);
  }

  @Test
  void shouldRouteMetricToDlqWhenPrimarySendFailsAsync() {
    MetricRequest request =
        MetricRequest.builder()
            .serviceId("payment-service")
            .metricName("latency_ms")
            .value(640.0)
            .unit("ms")
            .timestamp(1700000005000L)
            .build();

    CompletableFuture<SendResult<String, SentinelMetric>> failedPrimaryFuture =
        new CompletableFuture<>();
    failedPrimaryFuture.completeExceptionally(new RuntimeException("Broker unavailable"));

    when(kafkaTemplate.send(eq(METRICS_TOPIC), eq("payment-service"), any(SentinelMetric.class)))
        .thenReturn(failedPrimaryFuture);
    when(kafkaTemplate.send(eq(DLQ_TOPIC), eq("payment-service"), any(SentinelMetric.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    producerService.sendMetric(request);

    verify(kafkaTemplate).send(eq(METRICS_TOPIC), eq("payment-service"), any(SentinelMetric.class));
    verify(kafkaTemplate).send(eq(DLQ_TOPIC), eq("payment-service"), any(SentinelMetric.class));
  }

  @Test
  void shouldWrapSerializationExceptionIntoMetricProcessingException() {
    MetricRequest request =
        MetricRequest.builder()
            .serviceId("order-service")
            .metricName("memory_usage")
            .value(88.0)
            .unit("percent")
            .timestamp(1700000010000L)
            .build();

    when(kafkaTemplate.send(eq(METRICS_TOPIC), eq("order-service"), any(SentinelMetric.class)))
        .thenThrow(new SerializationException("Schema Registry unreachable"));

    assertThatThrownBy(() -> producerService.sendMetric(request))
        .isInstanceOf(MetricProcessingException.class)
        .hasMessageContaining("Failed to serialize metric");
  }

  @Test
  void shouldRejectNullOrBlankMetricFields() {
    assertThatThrownBy(() -> producerService.sendMetric(null))
        .isInstanceOf(MetricProcessingException.class);

    MetricRequest blankService =
        MetricRequest.builder()
            .serviceId("  ")
            .metricName("cpu_usage")
            .value(50.0)
            .timestamp(1000L)
            .build();
    assertThatThrownBy(() -> producerService.sendMetric(blankService))
        .isInstanceOf(MetricProcessingException.class);

    MetricRequest blankMetric =
        MetricRequest.builder()
            .serviceId("auth-service")
            .metricName("")
            .value(50.0)
            .timestamp(1000L)
            .build();
    assertThatThrownBy(() -> producerService.sendMetric(blankMetric))
        .isInstanceOf(MetricProcessingException.class);

    verify(kafkaTemplate, never()).send(any(), any(), any());
  }
}
