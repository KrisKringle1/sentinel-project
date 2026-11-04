export type ServiceStatus = "HEALTHY" | "WARNING" | "CRITICAL";

export type AlertSeverity = "WARNING" | "CRITICAL";

export type MetricName =
  | "cpu_usage"
  | "memory_usage"
  | "latency_ms"
  | "error_rate"
  | string;

/**
 * Mirrors com.sentinel.consumer.dto.MetricSummary
 */
export interface MetricSummary {
  metricName: string;
  count: number;
  min: number;
  max: number;
  movingAverage: number;
  lastValue: number;
  lastUpdated: number;
}

/**
 * Mirrors com.sentinel.consumer.dto.ServiceTelemetrySummary
 */
export interface ServiceTelemetrySummary {
  serviceId: string;
  status: ServiceStatus;
  totalEvents: number;
  anomalyCount: number;
  lastSeenTimestamp: number;
  metrics: Record<string, MetricSummary>;
}

/**
 * Mirrors com.sentinel.consumer.dto.AnomalyAlert
 */
export interface AnomalyAlert {
  alertId: string;
  serviceId: string;
  metricName: string;
  value: number;
  threshold: number;
  severity: AlertSeverity;
  message: string;
  timestamp: number;
}

/**
 * Mirrors com.sentinel.consumer.dto.DlqMetricRecord
 */
export interface DlqMetricRecord {
  serviceId: string;
  metricName: string;
  value: number;
  metricTimestamp: number;
  receivedAtTimestamp: number;
}

/**
 * Mirrors com.sentinel.consumer.dto.TelemetryStreamEvent
 */
export interface TelemetryStreamEvent {
  eventType: "METRIC" | "ALERT";
  serviceId: string;
  metricName: string;
  value: number;
  timestamp: number;
  serviceSummary: ServiceTelemetrySummary;
  alert?: AnomalyAlert | null;
}

/**
 * Mirrors com.sentinel.ingestion.dto.MetricRequest
 */
export interface MetricRequestPayload {
  serviceId: string;
  metricName: string;
  value: number;
  unit: string;
  timestamp: number;
}

export interface TimeSeriesPoint {
  id: string;
  serviceId: string;
  metricName: string;
  value: number;
  movingAverage?: number;
  timestamp: number;
  isAnomaly: boolean;
}

export type StreamConnectionState =
  | "CONNECTING"
  | "CONNECTED"
  | "RECONNECTING"
  | "DISCONNECTED";

export interface MetricMetadata {
  key: string;
  label: string;
  unit: string;
  shortUnit: string;
  threshold: number;
  criticalThreshold: number;
  normalMin: number;
  normalMax: number;
  anomalySpikeValue: number;
  description: string;
}

/**
 * Matches defaultThresholds() in SentinelConsumerProperties.java
 */
export const METRIC_CATALOG: Record<string, MetricMetadata> = {
  cpu_usage: {
    key: "cpu_usage",
    label: "CPU Usage",
    unit: "percent",
    shortUnit: "%",
    threshold: 90.0,
    criticalThreshold: 95.0,
    normalMin: 22.0,
    normalMax: 68.0,
    anomalySpikeValue: 96.4,
    description: "Container CPU utilization percentage",
  },
  memory_usage: {
    key: "memory_usage",
    label: "Memory Usage",
    unit: "percent",
    shortUnit: "%",
    threshold: 85.0,
    criticalThreshold: 95.0,
    normalMin: 38.0,
    normalMax: 74.0,
    anomalySpikeValue: 95.8,
    description: "JVM heap and resident memory utilization",
  },
  latency_ms: {
    key: "latency_ms",
    label: "P99 Latency",
    unit: "ms",
    shortUnit: "ms",
    threshold: 500.0,
    criticalThreshold: 575.0,
    normalMin: 35.0,
    normalMax: 210.0,
    anomalySpikeValue: 745.0,
    description: "End-to-end request response latency in milliseconds",
  },
  error_rate: {
    key: "error_rate",
    label: "Error Rate",
    unit: "percent",
    shortUnit: "%",
    threshold: 5.0,
    criticalThreshold: 5.75,
    normalMin: 0.1,
    normalMax: 1.8,
    anomalySpikeValue: 11.4,
    description: "HTTP 5xx error percentage across rolling window",
  },
};

export const SIMULATED_SERVICES = [
  "auth-service",
  "payment-service",
  "order-service",
  "inventory-service",
] as const;
