package com.sentinel.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

@Builder
public record MetricRequest(
    @NotBlank(message = "serviceId cannot be empty") @JsonProperty("serviceId") String serviceId,
    @NotBlank(message = "metricName cannot be empty") @JsonProperty("metricName") String metricName,
    @NotNull(message = "value cannot be null") @JsonProperty("value") Double value,
    @JsonProperty("unit") String unit,
    @NotNull(message = "timestamp cannot be null") @JsonProperty("timestamp") Long timestamp) {}
