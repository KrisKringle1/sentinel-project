package com.sentinel.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.common.avro.SentinelMetric;
import com.sentinel.consumer.config.SentinelConsumerProperties;
import com.sentinel.consumer.dto.AnomalyAlert;
import com.sentinel.consumer.dto.DlqMetricRecord;
import com.sentinel.consumer.dto.MetricSummary;
import com.sentinel.consumer.dto.ServiceTelemetrySummary;
import com.sentinel.consumer.dto.TelemetryStreamEvent;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TelemetryStateService {

  static final String REDIS_SERVICES_HASH_KEY = "sentinel:telemetry:services";
  static final String REDIS_ALERTS_LIST_KEY = "sentinel:telemetry:alerts";

  private final SentinelConsumerProperties properties;
  private final StringRedisTemplate redisTemplate;
  private final ObjectMapper objectMapper;

  private final Map<String, ServiceAggregate> serviceAggregates = new ConcurrentHashMap<>();
  private final Deque<AnomalyAlert> recentAlerts = new ConcurrentLinkedDeque<>();
  private final Deque<DlqMetricRecord> dlqRecords = new ConcurrentLinkedDeque<>();
  private final AtomicBoolean redisFallbackLogged = new AtomicBoolean(false);

  @Autowired
  public TelemetryStateService(
      SentinelConsumerProperties properties,
      @Autowired(required = false) StringRedisTemplate redisTemplate,
      ObjectMapper objectMapper) {
    this.properties = properties;
    this.redisTemplate = redisTemplate;
    this.objectMapper = objectMapper;
  }

  public TelemetryStreamEvent recordMetric(SentinelMetric metric) {
    if (metric == null || metric.getServiceId() == null || metric.getMetricName() == null) {
      throw new IllegalArgumentException("Metric, serviceId, and metricName must not be null");
    }

    String serviceId = metric.getServiceId().toString();
    String metricName = metric.getMetricName().toString();
    double value = metric.getValue();
    long timestamp = metric.getTimestamp() > 0 ? metric.getTimestamp() : System.currentTimeMillis();

    int windowSize = Math.max(1, properties.getWindowSize());
    ServiceAggregate aggregate =
        serviceAggregates.computeIfAbsent(serviceId, id -> new ServiceAggregate(id, windowSize));

    Optional<AnomalyAlert> maybeAlert;
    ServiceTelemetrySummary summary;

    synchronized (aggregate) {
      maybeAlert = aggregate.record(metricName, value, timestamp, properties.getThresholds());
      summary = aggregate.toSummary();
    }

    AnomalyAlert triggeredAlert = null;
    if (maybeAlert.isPresent()) {
      triggeredAlert = maybeAlert.get();
      addAlert(triggeredAlert);
      persistAlertToRedis(triggeredAlert);
    }

    persistServiceSummaryToRedis(summary);

    return TelemetryStreamEvent.builder()
        .eventType(triggeredAlert != null ? "ALERT" : "METRIC")
        .serviceId(serviceId)
        .metricName(metricName)
        .value(value)
        .timestamp(timestamp)
        .serviceSummary(summary)
        .alert(triggeredAlert)
        .build();
  }

  public DlqMetricRecord recordDlqMetric(SentinelMetric metric) {
    if (metric == null) {
      throw new IllegalArgumentException("DLQ metric must not be null");
    }

    DlqMetricRecord record =
        DlqMetricRecord.builder()
            .serviceId(metric.getServiceId() != null ? metric.getServiceId().toString() : "unknown")
            .metricName(
                metric.getMetricName() != null ? metric.getMetricName().toString() : "unknown")
            .value(metric.getValue())
            .metricTimestamp(metric.getTimestamp())
            .receivedAtTimestamp(System.currentTimeMillis())
            .build();

    dlqRecords.addFirst(record);
    int maxRecords = Math.max(1, properties.getMaxAlerts());
    while (dlqRecords.size() > maxRecords) {
      dlqRecords.pollLast();
    }
    return record;
  }

  public List<ServiceTelemetrySummary> getAllServices() {
    if (!serviceAggregates.isEmpty()) {
      return serviceAggregates.values().stream()
          .map(
              agg -> {
                synchronized (agg) {
                  return agg.toSummary();
                }
              })
          .sorted(Comparator.comparing(ServiceTelemetrySummary::serviceId))
          .toList();
    }
    return loadServicesFromRedis();
  }

  public Optional<ServiceTelemetrySummary> getService(String serviceId) {
    ServiceAggregate aggregate = serviceAggregates.get(serviceId);
    if (aggregate != null) {
      synchronized (aggregate) {
        return Optional.of(aggregate.toSummary());
      }
    }
    return loadServiceFromRedis(serviceId);
  }

  public List<AnomalyAlert> getAlerts(String serviceId, int limit) {
    int effectiveLimit = limit > 0 ? limit : properties.getMaxAlerts();
    List<AnomalyAlert> source =
        !recentAlerts.isEmpty() ? new ArrayList<>(recentAlerts) : loadAlertsFromRedis();

    return source.stream()
        .filter(
            alert ->
                serviceId == null
                    || serviceId.isBlank()
                    || serviceId.equalsIgnoreCase(alert.serviceId()))
        .limit(effectiveLimit)
        .toList();
  }

  public List<DlqMetricRecord> getDlqRecords() {
    return List.copyOf(dlqRecords);
  }

  private void addAlert(AnomalyAlert alert) {
    recentAlerts.addFirst(alert);
    int maxAlerts = Math.max(1, properties.getMaxAlerts());
    while (recentAlerts.size() > maxAlerts) {
      recentAlerts.pollLast();
    }
  }

  private void persistServiceSummaryToRedis(ServiceTelemetrySummary summary) {
    if (redisTemplate == null) {
      return;
    }
    try {
      String json = objectMapper.writeValueAsString(summary);
      redisTemplate.opsForHash().put(REDIS_SERVICES_HASH_KEY, summary.serviceId(), json);
    } catch (Exception ex) {
      logRedisFallbackOnce(ex);
    }
  }

  private void persistAlertToRedis(AnomalyAlert alert) {
    if (redisTemplate == null) {
      return;
    }
    try {
      String json = objectMapper.writeValueAsString(alert);
      redisTemplate.opsForList().leftPush(REDIS_ALERTS_LIST_KEY, json);
      redisTemplate.opsForList().trim(REDIS_ALERTS_LIST_KEY, 0, properties.getMaxAlerts() - 1L);
    } catch (Exception ex) {
      logRedisFallbackOnce(ex);
    }
  }

  private List<ServiceTelemetrySummary> loadServicesFromRedis() {
    if (redisTemplate == null) {
      return List.of();
    }
    try {
      Map<Object, Object> entries = redisTemplate.opsForHash().entries(REDIS_SERVICES_HASH_KEY);
      if (entries == null || entries.isEmpty()) {
        return List.of();
      }
      List<ServiceTelemetrySummary> result = new ArrayList<>();
      for (Object value : entries.values()) {
        if (value instanceof String json) {
          result.add(objectMapper.readValue(json, ServiceTelemetrySummary.class));
        }
      }
      result.sort(Comparator.comparing(ServiceTelemetrySummary::serviceId));
      return result;
    } catch (Exception ex) {
      logRedisFallbackOnce(ex);
      return List.of();
    }
  }

  private Optional<ServiceTelemetrySummary> loadServiceFromRedis(String serviceId) {
    if (redisTemplate == null) {
      return Optional.empty();
    }
    try {
      Object raw = redisTemplate.opsForHash().get(REDIS_SERVICES_HASH_KEY, serviceId);
      if (raw instanceof String json) {
        return Optional.of(objectMapper.readValue(json, ServiceTelemetrySummary.class));
      }
    } catch (Exception ex) {
      logRedisFallbackOnce(ex);
    }
    return Optional.empty();
  }

  private List<AnomalyAlert> loadAlertsFromRedis() {
    if (redisTemplate == null) {
      return List.of();
    }
    try {
      List<String> rawList =
          redisTemplate
              .opsForList()
              .range(REDIS_ALERTS_LIST_KEY, 0, properties.getMaxAlerts() - 1L);
      if (rawList == null || rawList.isEmpty()) {
        return List.of();
      }
      List<AnomalyAlert> alerts = new ArrayList<>(rawList.size());
      for (String json : rawList) {
        try {
          alerts.add(objectMapper.readValue(json, AnomalyAlert.class));
        } catch (JsonProcessingException ignored) {
          // Skip malformed entries
        }
      }
      return alerts;
    } catch (Exception ex) {
      logRedisFallbackOnce(ex);
      return List.of();
    }
  }

  private void logRedisFallbackOnce(Exception ex) {
    if (redisFallbackLogged.compareAndSet(false, true)) {
      log.warn(
          "Redis unavailable ({}). Falling back to in-memory telemetry state.", ex.getMessage());
    }
  }

  private static final class ServiceAggregate {
    private final String serviceId;
    private final int windowSize;
    private final Map<String, MetricWindow> metrics = new LinkedHashMap<>();
    private long totalEvents = 0;
    private long anomalyCount = 0;
    private long lastSeenTimestamp = 0;
    private String status = "HEALTHY";

    private ServiceAggregate(String serviceId, int windowSize) {
      this.serviceId = serviceId;
      this.windowSize = windowSize;
    }

    private Optional<AnomalyAlert> record(
        String metricName, double value, long timestamp, Map<String, Double> thresholds) {
      totalEvents++;
      lastSeenTimestamp = Math.max(lastSeenTimestamp, timestamp);

      MetricWindow window =
          metrics.computeIfAbsent(metricName, name -> new MetricWindow(name, windowSize));
      window.add(value, timestamp);

      Double threshold = thresholds != null ? thresholds.get(metricName) : null;
      AnomalyAlert alert = null;

      if (threshold != null && value > threshold) {
        anomalyCount++;
        String severity = determineSeverity(metricName, value, threshold);
        alert =
            AnomalyAlert.builder()
                .alertId(UUID.randomUUID().toString())
                .serviceId(serviceId)
                .metricName(metricName)
                .value(value)
                .threshold(threshold)
                .severity(severity)
                .message(
                    String.format(
                        "Service '%s' reported %s=%.2f (threshold: %.2f)",
                        serviceId, metricName, value, threshold))
                .timestamp(timestamp)
                .build();
      }

      recalculateServiceStatus(thresholds);
      return Optional.ofNullable(alert);
    }

    private void recalculateServiceStatus(Map<String, Double> thresholds) {
      if (thresholds == null || thresholds.isEmpty()) {
        status = "HEALTHY";
        return;
      }

      String worstStatus = "HEALTHY";
      for (Map.Entry<String, MetricWindow> entry : metrics.entrySet()) {
        Double threshold = thresholds.get(entry.getKey());
        double lastValue = entry.getValue().lastValue;
        if (threshold != null && lastValue > threshold) {
          String severity = determineSeverity(entry.getKey(), lastValue, threshold);
          if ("CRITICAL".equals(severity)) {
            worstStatus = "CRITICAL";
            break;
          }
          worstStatus = "WARNING";
        }
      }
      this.status = worstStatus;
    }

    private static String determineSeverity(String metricName, double value, double threshold) {
      if (("cpu_usage".equals(metricName) || "memory_usage".equals(metricName)) && value >= 95.0) {
        return "CRITICAL";
      }
      if (value >= threshold * 1.15) {
        return "CRITICAL";
      }
      return "WARNING";
    }

    private ServiceTelemetrySummary toSummary() {
      Map<String, MetricSummary> snapshot = new LinkedHashMap<>();
      for (Map.Entry<String, MetricWindow> entry : metrics.entrySet()) {
        snapshot.put(entry.getKey(), entry.getValue().toSummary());
      }
      return ServiceTelemetrySummary.builder()
          .serviceId(serviceId)
          .status(status)
          .totalEvents(totalEvents)
          .anomalyCount(anomalyCount)
          .lastSeenTimestamp(lastSeenTimestamp)
          .metrics(snapshot)
          .build();
    }
  }

  private static final class MetricWindow {
    private final String metricName;
    private final int maxWindowSize;
    private final Deque<Double> window = new ArrayDeque<>();
    private double windowSum = 0.0;
    private long count = 0;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private double lastValue = 0.0;
    private long lastUpdated = 0;

    private MetricWindow(String metricName, int maxWindowSize) {
      this.metricName = metricName;
      this.maxWindowSize = maxWindowSize;
    }

    private void add(double value, long timestamp) {
      count++;
      min = Math.min(min, value);
      max = Math.max(max, value);
      lastValue = value;
      lastUpdated = Math.max(lastUpdated, timestamp);

      window.addLast(value);
      windowSum += value;
      if (window.size() > maxWindowSize) {
        Double removed = window.removeFirst();
        if (removed != null) {
          windowSum -= removed;
        }
      }
    }

    private MetricSummary toSummary() {
      double avg = window.isEmpty() ? 0.0 : windowSum / window.size();
      double roundedAvg = Math.round(avg * 100.0) / 100.0;
      return MetricSummary.builder()
          .metricName(metricName)
          .count(count)
          .min(count == 0 ? 0.0 : min)
          .max(count == 0 ? 0.0 : max)
          .movingAverage(roundedAvg)
          .lastValue(lastValue)
          .lastUpdated(lastUpdated)
          .build();
    }
  }
}
