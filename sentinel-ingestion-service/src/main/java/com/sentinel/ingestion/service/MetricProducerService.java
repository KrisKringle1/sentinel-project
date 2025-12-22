package com.sentinel.ingestion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.ingestion.dto.MetricRequest;
import com.sentinel.ingestion.exception.MetricProcessingException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MetricProducerService {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;
  private final String topic;
  private final String dlqTopic;

  public MetricProducerService(
      KafkaTemplate<String, String> kafkaTemplate,
      ObjectMapper objectMapper,
      @Value("${sentinel.ingestion.topic.metrics:service-metrics}") String topic,
      @Value("${sentinel.ingestion.topic.metrics.dlq:service-metrics-dlq}") String dlqTopic) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
    this.topic = topic;
    this.dlqTopic = dlqTopic;
  }

  public CompletableFuture<SendResult<String, String>> sendMetric(MetricRequest request) {
    try {
      String payload = objectMapper.writeValueAsString(request);
      CompletableFuture<SendResult<String, String>> future =
          kafkaTemplate.send(topic, request.serviceId(), payload);

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
                    payload,
                    topic,
                    ex);
                sendToDlq(request.serviceId(), payload);
                return null;
              });
      return future;
    } catch (JsonProcessingException e) {
      log.error("Error converting MetricRequest to JSON", e);
      throw new MetricProcessingException("Failed to serialize metric to JSON", e);
    }
  }

  private void sendToDlq(String key, String payload) {
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
