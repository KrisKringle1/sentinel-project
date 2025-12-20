package com.sentinel.ingestion.controller;

import com.sentinel.ingestion.dto.MetricRequest;
import com.sentinel.ingestion.service.MetricProducerService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/metrics")
@RequiredArgsConstructor
public class MetricsController {

  private final MetricProducerService metricProducerService;

  @PostMapping
  public ResponseEntity<String> ingestMetric(@Valid @RequestBody MetricRequest request) {
    metricProducerService.sendMetric(request);

    return ResponseEntity.accepted().body("Metric received");
  }
}
