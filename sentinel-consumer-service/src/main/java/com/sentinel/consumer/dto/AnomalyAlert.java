package com.sentinel.consumer.dto;

import lombok.Builder;

@Builder
public record AnomalyAlert(
    String alertId,
    String serviceId,
    String metricName,
    double value,
    double threshold,
    String severity,
    String message,
    long timestamp) {}
