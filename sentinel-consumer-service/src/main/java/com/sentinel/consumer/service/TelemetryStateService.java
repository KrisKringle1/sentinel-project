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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TelemetryStateService {

  static final String REDIS_SERVICES_HASH_KEY = "sentinel:telemetry:services";
  static final String REDIS_ALERTS_LIST_KEY = "sentinel:telemetry:alerts";
  static final String REDIS_DLQ_LIST_KEY = "sentinel:telemetry:dlq";

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
        serviceAggregates.computeIfAbsent(serviceId, id -> loadAggregateState(id, windowSize));

    Optional<AnomalyAlert> maybeAlert;
    ServiceTelemetrySummary summary;
    PersistedServiceState persistedState;

    synchronized (aggregate) {
      maybeAlert = aggregate.record(metricName, value, timestamp, properties.getThresholds());
      summary = aggregate.toSummary();
      persistedState = aggregate.toPersistedState();
    }

    AnomalyAlert triggeredAlert = null;
    if (maybeAlert.isPresent()) {
      triggeredAlert = maybeAlert.get();
      addAlert(triggeredAlert);
      persistAlertToRedis(triggeredAlert);
    }

    persistServiceStateToRedis(persistedState);

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
    persistDlqRecordToRedis(record);
    return record;
  }

  public List<ServiceTelemetrySummary> getAllServices() {
    Map<String, ServiceTelemetrySummary> merged = new LinkedHashMap<>();
    for (ServiceTelemetrySummary summary : loadServicesFromRedis()) {
      merged.put(summary.serviceId(), summary);
    }
    serviceAggregates.forEach(
        (serviceId, aggregate) -> {
          synchronized (aggregate) {
            merged.put(serviceId, aggregate.toSummary());
          }
        });
    return merged.values().stream()
        .sorted(Comparator.comparing(ServiceTelemetrySummary::serviceId))
        .toList();
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
    List<AnomalyAlert> source = mergeAlerts();

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
    return mergeDlqRecords();
  }

  private void addAlert(AnomalyAlert alert) {
    recentAlerts.addFirst(alert);
    int maxAlerts = Math.max(1, properties.getMaxAlerts());
    while (recentAlerts.size() > maxAlerts) {
      recentAlerts.pollLast();
    }
  }

  private ServiceAggregate loadAggregateState(String serviceId, int windowSize) {
    return loadPersistedServiceStateFromRedis(serviceId)
        .map(state -> ServiceAggregate.fromPersisted(state, windowSize))
        .orElseGet(() -> new ServiceAggregate(serviceId, windowSize));
  }

  private void persistServiceStateToRedis(PersistedServiceState state) {
    if (redisTemplate == null) {
      return;
    }
    try {
      String json = objectMapper.writeValueAsString(state);
      redisTemplate.opsForHash().put(REDIS_SERVICES_HASH_KEY, state.serviceId(), json);
    } catch (Exception ex) {
      logRedisFallbackOnce(ex);
    }
  }

  private void persistAlertToRedis(AnomalyAlert alert) {
    pushToRedisList(REDIS_ALERTS_LIST_KEY, alert);
  }

  private void persistDlqRecordToRedis(DlqMetricRecord record) {
    pushToRedisList(REDIS_DLQ_LIST_KEY, record);
  }

  private void pushToRedisList(String key, Object item) {
    if (redisTemplate == null) {
      return;
    }
    try {
      String json = objectMapper.writeValueAsString(item);
      redisTemplate.opsForList().leftPush(key, json);
      redisTemplate.opsForList().trim(key, 0, Math.max(1, properties.getMaxAlerts()) - 1L);
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
          deserializeServiceSummary(json).ifPresent(result::add);
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
        return deserializeServiceSummary(json);
      }
    } catch (Exception ex) {
      logRedisFallbackOnce(ex);
    }
    return Optional.empty();
  }

  private Optional<ServiceTelemetrySummary> deserializeServiceSummary(String json) {
    try {
      PersistedServiceState state = objectMapper.readValue(json, PersistedServiceState.class);
      return Optional.of(
          ServiceAggregate.fromPersisted(state, Math.max(1, properties.getWindowSize()))
              .toSummary());
    } catch (JsonProcessingException ex) {
      try {
        return Optional.of(objectMapper.readValue(json, ServiceTelemetrySummary.class));
      } catch (JsonProcessingException ignored) {
        return Optional.empty();
      }
    }
  }

  private Optional<PersistedServiceState> loadPersistedServiceStateFromRedis(String serviceId) {
    if (redisTemplate == null) {
      return Optional.empty();
    }
    try {
      Object raw = redisTemplate.opsForHash().get(REDIS_SERVICES_HASH_KEY, serviceId);
      if (raw instanceof String json) {
        return Optional.of(objectMapper.readValue(json, PersistedServiceState.class));
      }
    } catch (Exception ex) {
      logRedisFallbackOnce(ex);
    }
    return Optional.empty();
  }

  private <T> List<T> loadListFromRedis(String key, Class<T> type) {
    if (redisTemplate == null) {
      return List.of();
    }
    try {
      List<String> rawList =
          redisTemplate.opsForList().range(key, 0, Math.max(1, properties.getMaxAlerts()) - 1L);
      if (rawList == null || rawList.isEmpty()) {
        return List.of();
      }
      List<T> items = new ArrayList<>(rawList.size());
      for (String json : rawList) {
        try {
          T item = objectMapper.readValue(json, type);
          if (item != null) {
            items.add(item);
          }
        } catch (JsonProcessingException ignored) {
          // Skip malformed entries
        }
      }
      return items;
    } catch (Exception ex) {
      logRedisFallbackOnce(ex);
      return List.of();
    }
  }

  private List<AnomalyAlert> mergeAlerts() {
    return mergeItems(
        recentAlerts,
        loadListFromRedis(REDIS_ALERTS_LIST_KEY, AnomalyAlert.class),
        TelemetryStateService::alertKey,
        (left, right) -> Long.compare(right.timestamp(), left.timestamp()));
  }

  private List<DlqMetricRecord> mergeDlqRecords() {
    int maxRecords = Math.max(1, properties.getMaxAlerts());
    return mergeItems(
            dlqRecords,
            loadListFromRedis(REDIS_DLQ_LIST_KEY, DlqMetricRecord.class),
            TelemetryStateService::dlqKey,
            (left, right) -> Long.compare(right.receivedAtTimestamp(), left.receivedAtTimestamp()))
        .stream()
        .limit(maxRecords)
        .toList();
  }

  private static <T> List<T> mergeItems(
      Iterable<T> localItems,
      List<T> redisItems,
      Function<T, String> keyExtractor,
      Comparator<T> comparator) {
    List<T> merged = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    for (T item : localItems) {
      if (seen.add(keyExtractor.apply(item))) {
        merged.add(item);
      }
    }
    for (T item : redisItems) {
      if (seen.add(keyExtractor.apply(item))) {
        merged.add(item);
      }
    }
    merged.sort(comparator);
    return merged;
  }

  private static String alertKey(AnomalyAlert alert) {
    if (alert.alertId() != null && !alert.alertId().isBlank()) {
      return alert.alertId();
    }
    return String.format(
        "%s|%s|%d|%s", alert.serviceId(), alert.metricName(), alert.timestamp(), alert.severity());
  }

  private static String dlqKey(DlqMetricRecord record) {
    return String.format(
        "%s|%s|%d|%d",
        record.serviceId(),
        record.metricName(),
        record.metricTimestamp(),
        record.receivedAtTimestamp());
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

    private static ServiceAggregate fromPersisted(PersistedServiceState state, int windowSize) {
      ServiceAggregate aggregate = new ServiceAggregate(state.serviceId(), windowSize);
      aggregate.totalEvents = state.totalEvents();
      aggregate.anomalyCount = state.anomalyCount();
      aggregate.lastSeenTimestamp = state.lastSeenTimestamp();
      aggregate.status = state.status() != null ? state.status() : "HEALTHY";
      if (state.metrics() != null) {
        for (Map.Entry<String, PersistedMetricState> entry : state.metrics().entrySet()) {
          if (entry.getKey() != null && entry.getValue() != null) {
            aggregate.metrics.put(
                entry.getKey(),
                MetricWindow.fromPersisted(entry.getKey(), windowSize, entry.getValue()));
          }
        }
      }
      return aggregate;
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

    private PersistedServiceState toPersistedState() {
      Map<String, PersistedMetricState> snapshot = new LinkedHashMap<>();
      for (Map.Entry<String, MetricWindow> entry : metrics.entrySet()) {
        snapshot.put(entry.getKey(), entry.getValue().toPersistedState());
      }
      return new PersistedServiceState(
          serviceId, status, totalEvents, anomalyCount, lastSeenTimestamp, snapshot);
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

    private PersistedMetricState toPersistedState() {
      return new PersistedMetricState(
          count,
          count == 0 ? 0.0 : min,
          count == 0 ? 0.0 : max,
          lastValue,
          lastUpdated,
          List.copyOf(window));
    }

    private static MetricWindow fromPersisted(
        String metricName, int maxWindowSize, PersistedMetricState persistedState) {
      MetricWindow window = new MetricWindow(metricName, maxWindowSize);
      window.count = Math.max(0, persistedState.count());
      if (window.count == 0) {
        window.min = Double.POSITIVE_INFINITY;
        window.max = Double.NEGATIVE_INFINITY;
      } else {
        window.min = persistedState.min();
        window.max = persistedState.max();
      }
      window.lastValue = persistedState.lastValue();
      window.lastUpdated = persistedState.lastUpdated();
      if (persistedState.windowValues() != null) {
        for (Double value : persistedState.windowValues()) {
          if (value != null) {
            window.window.addLast(value);
            window.windowSum += value;
          }
        }
      }
      while (window.window.size() > maxWindowSize) {
        Double removed = window.window.removeFirst();
        if (removed != null) {
          window.windowSum -= removed;
        }
      }
      if (!window.window.isEmpty()) {
        window.lastValue = window.window.peekLast();
      }
      return window;
    }
  }

  private record PersistedServiceState(
      String serviceId,
      String status,
      long totalEvents,
      long anomalyCount,
      long lastSeenTimestamp,
      Map<String, PersistedMetricState> metrics) {}

  private record PersistedMetricState(
      long count,
      double min,
      double max,
      double lastValue,
      long lastUpdated,
      List<Double> windowValues) {}
}
