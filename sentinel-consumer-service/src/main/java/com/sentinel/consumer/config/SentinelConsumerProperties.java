package com.sentinel.consumer.config;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "sentinel.consumer")
public class SentinelConsumerProperties {

  private Topic topic = new Topic();
  private int windowSize = 50;
  private int maxAlerts = 100;
  private Map<String, Double> thresholds = defaultThresholds();

  private static Map<String, Double> defaultThresholds() {
    Map<String, Double> defaults = new HashMap<>();
    defaults.put("cpu_usage", 90.0);
    defaults.put("memory_usage", 85.0);
    defaults.put("latency_ms", 500.0);
    defaults.put("error_rate", 5.0);
    return defaults;
  }

  @Getter
  @Setter
  public static class Topic {
    private String metrics = "service-metrics";
    private String metricsDlq = "service-metrics-dlq";
  }
}
