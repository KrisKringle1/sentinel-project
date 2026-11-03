package com.sentinel.consumer.dto;

import java.util.Map;
import lombok.Builder;

@Builder
public record ServiceTelemetrySummary(
    String serviceId,
    String status,
    long totalEvents,
    long anomalyCount,
    long lastSeenTimestamp,
    Map<String, MetricSummary> metrics) {}
