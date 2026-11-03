package com.sentinel.consumer.service;

import com.sentinel.common.avro.SentinelMetric;
import com.sentinel.consumer.dto.DlqMetricRecord;
import com.sentinel.consumer.dto.TelemetryStreamEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class MetricConsumerService {

  private final TelemetryStateService telemetryStateService;
  private final SseEmitterService sseEmitterService;

  @KafkaListener(
      topics = "${sentinel.consumer.topic.metrics:service-metrics}",
      groupId = "${spring.kafka.consumer.group-id:sentinel-metrics-consumer}")
  public void consumeMetric(SentinelMetric metric) {
    if (metric == null) {
      log.warn("Received null metric record from Kafka; ignoring.");
      return;
    }

    log.debug(
        "Consumed metric: serviceId=[{}] metricName=[{}] value=[{}] timestamp=[{}]",
        metric.getServiceId(),
        metric.getMetricName(),
        metric.getValue(),
        metric.getTimestamp());

    TelemetryStreamEvent streamEvent = telemetryStateService.recordMetric(metric);
    if (streamEvent.alert() != null) {
      log.warn("Anomaly detected: {}", streamEvent.alert().message());
    }

    sseEmitterService.broadcast(streamEvent);
  }

  @KafkaListener(
      topics = "${sentinel.consumer.topic.metrics-dlq:service-metrics-dlq}",
      groupId = "${spring.kafka.consumer.group-id:sentinel-metrics-consumer}-dlq")
  public void consumeDlqMetric(SentinelMetric metric) {
    if (metric == null) {
      return;
    }

    DlqMetricRecord dlqRecord = telemetryStateService.recordDlqMetric(metric);
    log.error(
        "Recorded dead-letter metric from DLQ: serviceId=[{}] metricName=[{}] value=[{}]",
        dlqRecord.serviceId(),
        dlqRecord.metricName(),
        dlqRecord.value());
  }
}
