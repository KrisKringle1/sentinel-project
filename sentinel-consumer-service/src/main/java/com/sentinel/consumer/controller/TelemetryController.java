package com.sentinel.consumer.controller;

import com.sentinel.consumer.dto.AnomalyAlert;
import com.sentinel.consumer.dto.DlqMetricRecord;
import com.sentinel.consumer.dto.ServiceTelemetrySummary;
import com.sentinel.consumer.service.SseEmitterService;
import com.sentinel.consumer.service.TelemetryStateService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/v1/telemetry")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class TelemetryController {

  private final TelemetryStateService telemetryStateService;
  private final SseEmitterService sseEmitterService;

  @GetMapping("/services")
  public ResponseEntity<List<ServiceTelemetrySummary>> getServices() {
    return ResponseEntity.ok(telemetryStateService.getAllServices());
  }

  @GetMapping("/services/{serviceId}")
  public ResponseEntity<ServiceTelemetrySummary> getServiceById(
      @PathVariable("serviceId") String serviceId) {
    return telemetryStateService
        .getService(serviceId)
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  @GetMapping("/alerts")
  public ResponseEntity<List<AnomalyAlert>> getAlerts(
      @RequestParam(name = "serviceId", required = false) String serviceId,
      @RequestParam(name = "limit", defaultValue = "50") int limit) {
    return ResponseEntity.ok(telemetryStateService.getAlerts(serviceId, limit));
  }

  @GetMapping("/dlq")
  public ResponseEntity<List<DlqMetricRecord>> getDlqRecords() {
    return ResponseEntity.ok(telemetryStateService.getDlqRecords());
  }

  @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public SseEmitter streamTelemetry() {
    return sseEmitterService.createEmitter();
  }
}
