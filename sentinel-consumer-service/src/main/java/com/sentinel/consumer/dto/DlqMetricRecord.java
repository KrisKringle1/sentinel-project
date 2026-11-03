package com.sentinel.consumer.dto;

import lombok.Builder;

@Builder
public record DlqMetricRecord(
    String serviceId,
    String metricName,
    double value,
    long metricTimestamp,
    long receivedAtTimestamp) {}
