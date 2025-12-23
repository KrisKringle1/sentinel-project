package com.sentinel.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

@Builder
public record TelemetryRequest(
    @NotBlank(message = "systemId cannot be empty") @JsonProperty("systemId") String systemId,
    @NotNull(message = "cpuUsage cannot be null") @JsonProperty("cpuUsage") Double cpuUsage,
    @NotNull(message = "memoryUsage cannot be null") @JsonProperty("memoryUsage")
        Double memoryUsage) {}
