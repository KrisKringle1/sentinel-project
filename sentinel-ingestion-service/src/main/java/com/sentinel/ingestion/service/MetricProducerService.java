package com.sentinel.ingestion.service;

import com.sentinel.common.avro.SentinelMetric;
import com.sentinel.ingestion.dto.MetricRequest;
import com.sentinel.ingestion.exception.MetricProcessingException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MetricProducerService {

  private final KafkaTemplate<String, SentinelMetric> kafkaTemplate;
  private final String topic;
  private final String dlqTopic;

  public MetricProducerService(
      KafkaTemplate<String, SentinelMetric> kafkaTemplate,
      @Value("${sentinel.ingestion.topic.metrics:service-metrics}") String topic,
      @Value("${sentinel.ingestion.topic.metrics.dlq:service-metrics-dlq}") String dlqTopic) {
    this.kafkaTemplate = kafkaTemplate;
    this.topic = topic;
    this.dlqTopic = dlqTopic;
  }

  public CompletableFuture<SendResult<String, SentinelMetric>> sendMetric(MetricRequest request) {
    if (request == null) {
      throw new MetricProcessingException("MetricRequest cannot be null");
    }

    if (request.serviceId() == null || request.serviceId().isBlank()) {
      throw new MetricProcessingException("ServiceId cannot be null or empty");
    }

    if (request.metricName() == null || request.metricName().isBlank()) {
      throw new MetricProcessingException("MetricName cannot be null or empty");
    }

    SentinelMetric metric =
        SentinelMetric.newBuilder()
            .setServiceId(request.serviceId())
            .setMetricName(request.metricName())
            .setValue(request.value())
            .setTimestamp(request.timestamp())
            .build();

    try {
      CompletableFuture<SendResult<String, SentinelMetric>> future =
          kafkaTemplate.send(topic, request.serviceId(), metric);

      future
          .thenAccept(
              result ->
                  log.info(
                      "Sent message with key=[{}] to topic=[{}] partition=[{}] offset=[{}]",
                      result.getProducerRecord().key(),
                      result.getRecordMetadata().topic(),
                      result.getRecordMetadata().partition(),
                      result.getRecordMetadata().offset()))
          .exceptionally(
              ex -> {
                log.error(
                    "Unable to send message=[{}] to topic=[{}]. Sending to DLQ...",
                    metric,
                    topic,
                    ex);
                sendToDlq(request.serviceId(), metric);
                return null;
              });
      return future;
    } catch (SerializationException e) {
      log.error("Serialization failed for metric request", e);
      throw new MetricProcessingException("Failed to serialize metric", e);
    } catch (RuntimeException e) {
      log.error("Error processing metric request", e);
      throw new MetricProcessingException("Failed to process metric", e);
    }
  }

  private void sendToDlq(String key, SentinelMetric payload) {
    kafkaTemplate
        .send(dlqTopic, key, payload)
        .whenComplete(
            (result, ex) -> {
              if (ex != null) {
                log.error("CRITICAL: Failed to send message to DLQ topic=[{}]", dlqTopic, ex);
              } else {
                log.info("Sent message to DLQ topic=[{}]", dlqTopic);
              }
            });
  }
}
