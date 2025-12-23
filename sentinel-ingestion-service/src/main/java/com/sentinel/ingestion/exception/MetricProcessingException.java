package com.sentinel.ingestion.exception;

public class MetricProcessingException extends RuntimeException {

  public MetricProcessingException(String message) {
    super(message);
  }

  public MetricProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}
