package com.sentinel.consumer.dto;

import lombok.Builder;

@Builder
public record TelemetryStreamEvent(
    String eventType,
    String serviceId,
    String metricName,
    Double value,
    Long timestamp,
    ServiceTelemetrySummary serviceSummary,
    AnomalyAlert alert) {}
