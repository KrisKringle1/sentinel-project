package com.sentinel.consumer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.common.avro.SentinelMetric;
import com.sentinel.consumer.config.SentinelConsumerProperties;
import com.sentinel.consumer.dto.AnomalyAlert;
import com.sentinel.consumer.dto.DlqMetricRecord;
import com.sentinel.consumer.dto.MetricSummary;
import com.sentinel.consumer.dto.ServiceTelemetrySummary;
import com.sentinel.consumer.dto.TelemetryStreamEvent;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

class TelemetryStateServiceTest {

  private SentinelConsumerProperties properties;
  private ObjectMapper objectMapper;
  private TelemetryStateService stateService;

  @BeforeEach
  void setUp() {
    properties = new SentinelConsumerProperties();
    properties.setWindowSize(3);
    properties.setMaxAlerts(5);
    objectMapper = new ObjectMapper();
    stateService = new TelemetryStateService(properties, null, objectMapper);
  }

  @Test
  void shouldAggregateRollingWindowMetricsAccurately() {
    stateService.recordMetric(buildMetric("auth-service", "cpu_usage", 20.0, 1000L));
    stateService.recordMetric(buildMetric("auth-service", "cpu_usage", 40.0, 2000L));
    stateService.recordMetric(buildMetric("auth-service", "cpu_usage", 60.0, 3000L));
    // Window size is 3, so adding 80.0 evicts 20.0 from moving average (40 + 60 + 80) / 3 = 60.0
    TelemetryStreamEvent event =
        stateService.recordMetric(buildMetric("auth-service", "cpu_usage", 80.0, 4000L));

    assertThat(event.eventType()).isEqualTo("METRIC");
    assertThat(event.alert()).isNull();

    ServiceTelemetrySummary summary = event.serviceSummary();
    assertThat(summary.serviceId()).isEqualTo("auth-service");
    assertThat(summary.status()).isEqualTo("HEALTHY");
    assertThat(summary.totalEvents()).isEqualTo(4);
    assertThat(summary.anomalyCount()).isZero();
    assertThat(summary.lastSeenTimestamp()).isEqualTo(4000L);

    MetricSummary cpuSummary = summary.metrics().get("cpu_usage");
    assertThat(cpuSummary).isNotNull();
    assertThat(cpuSummary.count()).isEqualTo(4);
    assertThat(cpuSummary.min()).isEqualTo(20.0);
    assertThat(cpuSummary.max()).isEqualTo(80.0);
    assertThat(cpuSummary.movingAverage()).isEqualTo(60.0);
    assertThat(cpuSummary.lastValue()).isEqualTo(80.0);
    assertThat(cpuSummary.lastUpdated()).isEqualTo(4000L);
  }

  @Test
  void shouldDetectAnomaliesAndRecoverServiceStatusWhenMetricsNormalize() {
    // 1. Trigger WARNING anomaly (cpu_usage = 92.0 > 90.0, < 95.0)
    TelemetryStreamEvent warningEvent =
        stateService.recordMetric(buildMetric("payment-service", "cpu_usage", 92.0, 1000L));

    assertThat(warningEvent.eventType()).isEqualTo("ALERT");
    assertThat(warningEvent.alert()).isNotNull();
    assertThat(warningEvent.alert().severity()).isEqualTo("WARNING");
    assertThat(warningEvent.serviceSummary().status()).isEqualTo("WARNING");

    // 2. Trigger CRITICAL anomaly (latency_ms = 650.0 >= 500.0 * 1.15)
    TelemetryStreamEvent criticalEvent =
        stateService.recordMetric(buildMetric("payment-service", "latency_ms", 650.0, 2000L));

    assertThat(criticalEvent.eventType()).isEqualTo("ALERT");
    assertThat(criticalEvent.alert()).isNotNull();
    assertThat(criticalEvent.alert().severity()).isEqualTo("CRITICAL");
    assertThat(criticalEvent.serviceSummary().status()).isEqualTo("CRITICAL");
    assertThat(criticalEvent.serviceSummary().anomalyCount()).isEqualTo(2);

    // 3. Normalize both metrics and verify status returns to HEALTHY
    stateService.recordMetric(buildMetric("payment-service", "cpu_usage", 45.0, 3000L));
    TelemetryStreamEvent recoveredEvent =
        stateService.recordMetric(buildMetric("payment-service", "latency_ms", 120.0, 4000L));

    assertThat(recoveredEvent.serviceSummary().status()).isEqualTo("HEALTHY");
    assertThat(stateService.getAlerts("payment-service", 10)).hasSize(2);
  }

  @Test
  void shouldPersistToRedisAndFallbackGracefullyOnFailure() {
    StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
    @SuppressWarnings("unchecked")
    ListOperations<String, String> listOps = mock(ListOperations.class);

    when(redisTemplate.opsForHash()).thenReturn(hashOps);
    when(redisTemplate.opsForList()).thenReturn(listOps);

    TelemetryStateService redisBackedService =
        new TelemetryStateService(properties, redisTemplate, objectMapper);

    redisBackedService.recordMetric(buildMetric("orders-service", "cpu_usage", 96.5, 5000L));

    verify(hashOps)
        .put(eq(TelemetryStateService.REDIS_SERVICES_HASH_KEY), eq("orders-service"), anyString());
    verify(listOps).leftPush(eq(TelemetryStateService.REDIS_ALERTS_LIST_KEY), anyString());
    verify(listOps).trim(eq(TelemetryStateService.REDIS_ALERTS_LIST_KEY), eq(0L), anyLong());

    // Simulate Redis failure and verify in-memory state continues working
    when(redisTemplate.opsForHash()).thenThrow(new RedisConnectionFailureException("Redis down"));
    TelemetryStreamEvent fallbackEvent =
        redisBackedService.recordMetric(buildMetric("orders-service", "cpu_usage", 50.0, 6000L));

    assertThat(fallbackEvent.serviceSummary().totalEvents()).isEqualTo(2);
    assertThat(redisBackedService.getAllServices()).hasSize(1);
    List<AnomalyAlert> alerts = redisBackedService.getAlerts("orders-service", 10);
    assertThat(alerts).hasSize(1);
  }

  @Test
  void shouldRecordDlqMetricsAndRejectNullMetric() {
    DlqMetricRecord dlqRecord =
        stateService.recordDlqMetric(buildMetric("search-service", "error_rate", 12.5, 7000L));

    assertThat(dlqRecord.serviceId()).isEqualTo("search-service");
    assertThat(stateService.getDlqRecords()).hasSize(1);

    assertThatThrownBy(() -> stateService.recordMetric(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldHydratePersistedAggregateStateAfterRestart() {
    StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
    @SuppressWarnings("unchecked")
    ListOperations<String, String> listOps = mock(ListOperations.class);
    when(redisTemplate.opsForHash()).thenReturn(hashOps);
    when(redisTemplate.opsForList()).thenReturn(listOps);

    TelemetryStateService firstInstance =
        new TelemetryStateService(properties, redisTemplate, objectMapper);
    firstInstance.recordMetric(buildMetric("auth-service", "cpu_usage", 40.0, 1000L));
    firstInstance.recordMetric(buildMetric("auth-service", "cpu_usage", 80.0, 2000L));

    org.mockito.ArgumentCaptor<String> persistedStateCaptor =
        org.mockito.ArgumentCaptor.forClass(String.class);
    verify(hashOps, atLeast(1))
        .put(
            eq(TelemetryStateService.REDIS_SERVICES_HASH_KEY),
            eq("auth-service"),
            persistedStateCaptor.capture());

    String persistedStateJson =
        persistedStateCaptor.getAllValues().get(persistedStateCaptor.getAllValues().size() - 1);
    when(hashOps.get(TelemetryStateService.REDIS_SERVICES_HASH_KEY, "auth-service"))
        .thenReturn(persistedStateJson);

    TelemetryStateService restartedInstance =
        new TelemetryStateService(properties, redisTemplate, objectMapper);
    TelemetryStreamEvent event =
        restartedInstance.recordMetric(buildMetric("auth-service", "cpu_usage", 60.0, 3000L));

    ServiceTelemetrySummary summary = event.serviceSummary();
    assertThat(summary.totalEvents()).isEqualTo(3);
    assertThat(summary.metrics().get("cpu_usage").count()).isEqualTo(3);
    assertThat(summary.metrics().get("cpu_usage").min()).isEqualTo(40.0);
    assertThat(summary.metrics().get("cpu_usage").max()).isEqualTo(80.0);
  }

  @Test
  void shouldMergeLocalAndRedisServiceSnapshots() throws Exception {
    StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
    @SuppressWarnings("unchecked")
    ListOperations<String, String> listOps = mock(ListOperations.class);
    when(redisTemplate.opsForHash()).thenReturn(hashOps);
    when(redisTemplate.opsForList()).thenReturn(listOps);

    TelemetryStateService redisBackedService =
        new TelemetryStateService(properties, redisTemplate, objectMapper);
    redisBackedService.recordMetric(buildMetric("local-service", "cpu_usage", 40.0, 1000L));

    ServiceTelemetrySummary redisOnlySummary =
        ServiceTelemetrySummary.builder()
            .serviceId("redis-only")
            .status("HEALTHY")
            .totalEvents(2)
            .anomalyCount(0)
            .lastSeenTimestamp(900L)
            .metrics(Map.of())
            .build();

    when(hashOps.entries(TelemetryStateService.REDIS_SERVICES_HASH_KEY))
        .thenReturn(
            Map.of(
                "redis-only", objectMapper.writeValueAsString(redisOnlySummary),
                "local-service", objectMapper.writeValueAsString(redisOnlySummary)));

    List<ServiceTelemetrySummary> services = redisBackedService.getAllServices();

    assertThat(services)
        .extracting(ServiceTelemetrySummary::serviceId)
        .contains("local-service", "redis-only");
  }

  @Test
  void shouldMergeLocalAndRedisAlertHistory() throws Exception {
    StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
    @SuppressWarnings("unchecked")
    ListOperations<String, String> listOps = mock(ListOperations.class);
    when(redisTemplate.opsForHash()).thenReturn(hashOps);
    when(redisTemplate.opsForList()).thenReturn(listOps);
    when(hashOps.get(eq(TelemetryStateService.REDIS_SERVICES_HASH_KEY), anyString()))
        .thenReturn(null);

    TelemetryStateService redisBackedService =
        new TelemetryStateService(properties, redisTemplate, objectMapper);
    redisBackedService.recordMetric(buildMetric("billing-service", "cpu_usage", 96.0, 2000L));

    AnomalyAlert redisAlert =
        AnomalyAlert.builder()
            .alertId("redis-alert-1")
            .serviceId("orders-service")
            .metricName("latency_ms")
            .value(700.0)
            .threshold(500.0)
            .severity("CRITICAL")
            .message("redis alert")
            .timestamp(1000L)
            .build();

    when(listOps.range(
            TelemetryStateService.REDIS_ALERTS_LIST_KEY, 0, properties.getMaxAlerts() - 1L))
        .thenReturn(List.of(objectMapper.writeValueAsString(redisAlert)));

    List<AnomalyAlert> alerts = redisBackedService.getAlerts(null, 10);
    assertThat(alerts).extracting(AnomalyAlert::alertId).contains("redis-alert-1");
    assertThat(alerts).hasSizeGreaterThanOrEqualTo(2);
  }

  @Test
  void shouldPersistAndMergeDlqHistoryFromRedis() throws Exception {
    StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
    @SuppressWarnings("unchecked")
    ListOperations<String, String> listOps = mock(ListOperations.class);
    when(redisTemplate.opsForHash()).thenReturn(hashOps);
    when(redisTemplate.opsForList()).thenReturn(listOps);

    TelemetryStateService redisBackedService =
        new TelemetryStateService(properties, redisTemplate, objectMapper);
    redisBackedService.recordDlqMetric(buildMetric("local-dlq", "error_rate", 4.0, 2000L));

    DlqMetricRecord redisRecord =
        DlqMetricRecord.builder()
            .serviceId("redis-dlq")
            .metricName("error_rate")
            .value(5.0)
            .metricTimestamp(1000L)
            .receivedAtTimestamp(1001L)
            .build();

    when(listOps.range(TelemetryStateService.REDIS_DLQ_LIST_KEY, 0, properties.getMaxAlerts() - 1L))
        .thenReturn(List.of(objectMapper.writeValueAsString(redisRecord)));

    List<DlqMetricRecord> records = redisBackedService.getDlqRecords();
    assertThat(records).extracting(DlqMetricRecord::serviceId).contains("local-dlq", "redis-dlq");
    verify(listOps).leftPush(eq(TelemetryStateService.REDIS_DLQ_LIST_KEY), anyString());
  }

  private static SentinelMetric buildMetric(
      String serviceId, String metricName, double value, long timestamp) {
    return SentinelMetric.newBuilder()
        .setServiceId(serviceId)
        .setMetricName(metricName)
        .setValue(value)
        .setTimestamp(timestamp)
        .build();
  }
}
