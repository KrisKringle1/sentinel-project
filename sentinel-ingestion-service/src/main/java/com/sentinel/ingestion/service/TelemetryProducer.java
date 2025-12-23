package com.sentinel.ingestion.service;

import com.sentinel.schema.SystemTelemetry;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TelemetryProducer {

  // The Key is a String (System ID), the Value is our Avro object
  private final KafkaTemplate<String, SystemTelemetry> kafkaTemplate;
  @org.springframework.beans.factory.annotation.Value("${sentinel.ingestion.topic.telemetry:system-telemetry}")
  private String topic;
  public void sendTelemetry(SystemTelemetry data) {
    // We use the systemId as the message key to ensure
    // all data for one machine stays in the same partition (Order Guarantee)
    CompletableFuture<SendResult<String, SystemTelemetry>> future =
        kafkaTemplate.send(TOPIC, data.getSystemId().toString(), data);

    future.whenComplete(
        (result, ex) -> {
          if (ex == null) {
            log.info(
                "Sent message=[{}] with offset=[{}]", data, result.getRecordMetadata().offset());
          } else {
            log.error("Unable to send message=[{}] due to : {}", data, ex.getMessage());
          }
        });
  }
}
