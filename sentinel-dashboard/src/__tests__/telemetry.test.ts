import assert from "node:assert/strict";
import test from "node:test";
import {
  METRIC_CATALOG,
  SIMULATED_SERVICES,
} from "../types/telemetry";
import {
  CONSUMER_BASE_URL,
  INGESTION_BASE_URL,
  buildAlertsUrl,
  getTelemetryStreamUrl,
} from "../lib/api";

test("METRIC_CATALOG defines all 4 core telemetry metrics with valid threshold invariants", () => {
  const expectedMetrics = [
    "cpu_usage",
    "memory_usage",
    "latency_ms",
    "error_rate",
  ];

  assert.deepEqual(Object.keys(METRIC_CATALOG), expectedMetrics);

  for (const key of expectedMetrics) {
    const meta = METRIC_CATALOG[key];
    assert.equal(meta.key, key);
    assert.ok(meta.label.length > 0, `${key} label must not be empty`);
    assert.ok(meta.unit.length > 0, `${key} unit must not be empty`);
    assert.ok(
      meta.normalMin >= 0 && meta.normalMin < meta.normalMax,
      `${key} normalMin (${meta.normalMin}) must be < normalMax (${meta.normalMax})`
    );
    assert.ok(
      meta.normalMax < meta.threshold,
      `${key} normalMax (${meta.normalMax}) must be < threshold (${meta.threshold})`
    );
    assert.ok(
      meta.threshold < meta.criticalThreshold,
      `${key} threshold (${meta.threshold}) must be < criticalThreshold (${meta.criticalThreshold})`
    );
    assert.ok(
      meta.anomalySpikeValue > meta.threshold,
      `${key} anomalySpikeValue (${meta.anomalySpikeValue}) must breach threshold (${meta.threshold})`
    );
  }
});

test("SIMULATED_SERVICES contains non-empty unique service identifiers", () => {
  assert.ok(SIMULATED_SERVICES.length >= 4);
  const unique = new Set(SIMULATED_SERVICES);
  assert.equal(unique.size, SIMULATED_SERVICES.length);
});

test("API URL helpers resolve default ingestion, consumer, alerts, and SSE endpoints", () => {
  const expectedIngestionBase =
    process.env.NEXT_PUBLIC_INGESTION_URL || "http://localhost:8080";
  const expectedConsumerBase =
    process.env.NEXT_PUBLIC_CONSUMER_URL || "http://localhost:8081";

  assert.equal(INGESTION_BASE_URL, expectedIngestionBase);
  assert.equal(CONSUMER_BASE_URL, expectedConsumerBase);
  assert.equal(
    getTelemetryStreamUrl(),
    `${expectedConsumerBase}/api/v1/telemetry/stream`
  );
  assert.equal(
    buildAlertsUrl(undefined, 100),
    `${expectedConsumerBase}/api/v1/telemetry/alerts?limit=100`
  );
  assert.equal(
    buildAlertsUrl("auth-service", 25),
    `${expectedConsumerBase}/api/v1/telemetry/alerts?limit=25&serviceId=auth-service`
  );
});
