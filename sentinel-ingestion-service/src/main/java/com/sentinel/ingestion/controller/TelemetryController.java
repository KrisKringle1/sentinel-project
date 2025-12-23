package com.sentinel.ingestion.controller;

import com.sentinel.ingestion.dto.TelemetryRequest;
import com.sentinel.ingestion.service.TelemetryProducer;
import com.sentinel.schema.SystemTelemetry;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/telemetry")
@RequiredArgsConstructor
public class TelemetryController {

  private final TelemetryProducer telemetryProducer;

  @PostMapping
  public ResponseEntity<String> collectTelemetry(@RequestBody TelemetryRequest request) {
    // Staff-Level Mapping: Convert the incoming JSON DTO to our Avro Domain Object
    SystemTelemetry telemetry =
        SystemTelemetry.newBuilder()
            .setSystemId(request.systemId())
            .setCpuUsage(request.cpuUsage())
            .setMemoryUsage(request.memoryUsage())
            .setTimestamp(Instant.now().toEpochMilli())
            .build();

    telemetryProducer.sendTelemetry(telemetry);

    return ResponseEntity.accepted().body("Telemetry accepted for processing");
  }
}
