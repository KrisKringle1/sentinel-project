package com.sentinel.consumer.dto;

import lombok.Builder;

@Builder
public record MetricSummary(
    String metricName,
    long count,
    double min,
    double max,
    double movingAverage,
    double lastValue,
    long lastUpdated) {}
