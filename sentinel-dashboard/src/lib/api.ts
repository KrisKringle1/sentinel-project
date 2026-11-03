import {
  AnomalyAlert,
  DlqMetricRecord,
  MetricRequestPayload,
  ServiceTelemetrySummary,
} from "@/types/telemetry";

export const INGESTION_BASE_URL =
  process.env.NEXT_PUBLIC_INGESTION_URL || "http://localhost:8080";

export const CONSUMER_BASE_URL =
  process.env.NEXT_PUBLIC_CONSUMER_URL || "http://localhost:8081";

export function getTelemetryStreamUrl(): string {
  return `${CONSUMER_BASE_URL}/api/v1/telemetry/stream`;
}

export async function fetchServices(): Promise<ServiceTelemetrySummary[]> {
  const response = await fetch(`${CONSUMER_BASE_URL}/api/v1/telemetry/services`, {
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch services: HTTP ${response.status}`);
  }
  return response.json();
}

export async function fetchAlerts(
  serviceId?: string,
  limit = 50
): Promise<AnomalyAlert[]> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (serviceId) {
    params.set("serviceId", serviceId);
  }
  const response = await fetch(
    `${CONSUMER_BASE_URL}/api/v1/telemetry/alerts?${params.toString()}`,
    {
      cache: "no-store",
    }
  );
  if (!response.ok) {
    throw new Error(`Failed to fetch alerts: HTTP ${response.status}`);
  }
  return response.json();
}

export async function fetchDlqRecords(): Promise<DlqMetricRecord[]> {
  const response = await fetch(`${CONSUMER_BASE_URL}/api/v1/telemetry/dlq`, {
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch DLQ records: HTTP ${response.status}`);
  }
  return response.json();
}

export async function ingestMetric(
  payload: MetricRequestPayload
): Promise<string> {
  const response = await fetch(`${INGESTION_BASE_URL}/api/v1/metrics`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const errorText = await response.text().catch(() => "Unknown error");
    throw new Error(`Ingestion failed (HTTP ${response.status}): ${errorText}`);
  }

  return response.text();
}
