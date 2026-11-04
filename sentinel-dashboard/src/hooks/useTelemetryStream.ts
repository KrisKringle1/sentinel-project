"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AnomalyAlert,
  DlqMetricRecord,
  METRIC_CATALOG,
  ServiceTelemetrySummary,
  StreamConnectionState,
  TelemetryStreamEvent,
  TimeSeriesPoint,
} from "@/types/telemetry";
import {
  fetchAlerts,
  fetchDlqRecords,
  fetchServices,
  getTelemetryStreamUrl,
} from "@/lib/api";

const MAX_TIMESERIES_POINTS = 160;
const MAX_ALERTS_IN_FEED = 100;
const RECONNECT_DELAY_MS = 3000;

export const TELEMETRY_QUERY_KEYS = {
  all: ["telemetry"] as const,
  services: ["telemetry", "services"] as const,
  alerts: ["telemetry", "alerts"] as const,
  dlq: ["telemetry", "dlq"] as const,
};

export interface UseTelemetryStreamResult {
  services: ServiceTelemetrySummary[];
  alerts: AnomalyAlert[];
  dlqRecords: DlqMetricRecord[];
  timeSeries: TimeSeriesPoint[];
  connectionState: StreamConnectionState;
  lastEventTimestamp: number | null;
  eventsPerSecond: number;
  isLoadingInitial: boolean;
  isFetching: boolean;
  error: string | null;
  refreshSnapshot: () => Promise<void>;
}

export function useTelemetryStream(): UseTelemetryStreamResult {
  const queryClient = useQueryClient();

  const [timeSeries, setTimeSeries] = useState<TimeSeriesPoint[]>([]);
  const [connectionState, setConnectionState] =
    useState<StreamConnectionState>("CONNECTING");
  const [lastEventTimestamp, setLastEventTimestamp] = useState<number | null>(
    null
  );
  const [eventsPerSecond, setEventsPerSecond] = useState<number>(0);

  // Rolling 5-second event timestamps for live throughput calculation
  const recentEventTimesRef = useRef<number[]>([]);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // 1. Declarative TanStack Queries for initial hydration & background sync
  const servicesQuery = useQuery({
    queryKey: TELEMETRY_QUERY_KEYS.services,
    queryFn: fetchServices,
  });

  const alertsQuery = useQuery({
    queryKey: TELEMETRY_QUERY_KEYS.alerts,
    queryFn: () => fetchAlerts(undefined, MAX_ALERTS_IN_FEED),
  });

  const dlqQuery = useQuery({
    queryKey: TELEMETRY_QUERY_KEYS.dlq,
    queryFn: fetchDlqRecords,
    refetchInterval: 10_000,
  });

  // Seed initial time-series points when servicesQuery first resolves
  useEffect(() => {
    if (!servicesQuery.data || servicesQuery.data.length === 0) {
      return;
    }
    setTimeSeries((prev) => {
      if (prev.length > 0) {
        return prev;
      }
      const seeded: TimeSeriesPoint[] = [];
      for (const svc of servicesQuery.data) {
        for (const [metricName, summary] of Object.entries(svc.metrics || {})) {
          const threshold = METRIC_CATALOG[metricName]?.threshold;
          seeded.push({
            id: `seed-${svc.serviceId}-${metricName}-${summary.lastUpdated}`,
            serviceId: svc.serviceId,
            metricName,
            value: summary.lastValue,
            movingAverage: summary.movingAverage,
            timestamp: summary.lastUpdated || svc.lastSeenTimestamp,
            isAnomaly:
              threshold !== undefined && summary.lastValue > threshold,
          });
        }
      }
      return seeded.sort((a, b) => a.timestamp - b.timestamp);
    });
  }, [servicesQuery.data]);

  const refreshSnapshot = useCallback(async () => {
    await queryClient.invalidateQueries({
      queryKey: TELEMETRY_QUERY_KEYS.all,
    });
  }, [queryClient]);

  // Throughput ticker (updates events/sec every second)
  useEffect(() => {
    const interval = setInterval(() => {
      const cutoff = Date.now() - 5000;
      recentEventTimesRef.current = recentEventTimesRef.current.filter(
        (t) => t >= cutoff
      );
      const rate =
        Math.round((recentEventTimesRef.current.length / 5) * 10) / 10;
      setEventsPerSecond(rate);
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  // 2. Real-Time SSE EventSource pushing deltas directly into React Query Cache
  useEffect(() => {
    let eventSource: EventSource | null = null;
    let isUnmounted = false;

    const connectSse = () => {
      if (isUnmounted) return;
      setConnectionState((prev) =>
        prev === "CONNECTED" ? "CONNECTED" : "CONNECTING"
      );

      const streamUrl = getTelemetryStreamUrl();
      const es = new EventSource(streamUrl);
      eventSource = es;

      es.onopen = () => {
        if (isUnmounted) return;
        setConnectionState("CONNECTED");
      };

      es.addEventListener("connected", () => {
        if (isUnmounted) return;
        setConnectionState("CONNECTED");
      });

      es.addEventListener("telemetry", (rawEvent: MessageEvent) => {
        if (isUnmounted) return;
        try {
          const event: TelemetryStreamEvent = JSON.parse(rawEvent.data);
          const now = Date.now();
          recentEventTimesRef.current.push(now);
          setLastEventTimestamp(event.timestamp || now);

          // Update ["telemetry", "services"] cache in-place via setQueryData
          if (event.serviceSummary) {
            queryClient.setQueryData<ServiceTelemetrySummary[]>(
              TELEMETRY_QUERY_KEYS.services,
              (oldServices = []) => {
                const exists = oldServices.some(
                  (s) => s.serviceId === event.serviceId
                );
                const updated = exists
                  ? oldServices.map((s) =>
                      s.serviceId === event.serviceId
                        ? event.serviceSummary
                        : s
                    )
                  : [...oldServices, event.serviceSummary];
                return [...updated].sort((a, b) =>
                  a.serviceId.localeCompare(b.serviceId)
                );
              }
            );
          }

          const metricSummary =
            event.serviceSummary?.metrics?.[event.metricName];
          const threshold = METRIC_CATALOG[event.metricName]?.threshold;
          const isAnomaly =
            event.eventType === "ALERT" ||
            Boolean(event.alert) ||
            (threshold !== undefined && event.value > threshold);

          const point: TimeSeriesPoint = {
            id: `${event.serviceId}-${event.metricName}-${event.timestamp}-${Math.random()
              .toString(36)
              .slice(2, 7)}`,
            serviceId: event.serviceId,
            metricName: event.metricName,
            value: event.value,
            movingAverage: metricSummary?.movingAverage,
            timestamp: event.timestamp || now,
            isAnomaly,
          };

          setTimeSeries((prev) => {
            const next = [...prev, point];
            return next.length > MAX_TIMESERIES_POINTS
              ? next.slice(next.length - MAX_TIMESERIES_POINTS)
              : next;
          });

          // If the telemetry frame carries an anomaly alert, prepend it to ["telemetry", "alerts"] cache
          if (event.alert) {
            const incomingAlert = event.alert;
            queryClient.setQueryData<AnomalyAlert[]>(
              TELEMETRY_QUERY_KEYS.alerts,
              (oldAlerts = []) => {
                if (oldAlerts.some((a) => a.alertId === incomingAlert.alertId)) {
                  return oldAlerts;
                }
                return [incomingAlert, ...oldAlerts].slice(
                  0,
                  MAX_ALERTS_IN_FEED
                );
              }
            );
          }
        } catch {
          // Ignore malformed event frames
        }
      });

      es.onerror = () => {
        if (isUnmounted) return;
        setConnectionState("RECONNECTING");
        es.close();
        if (reconnectTimerRef.current) {
          clearTimeout(reconnectTimerRef.current);
        }
        reconnectTimerRef.current = setTimeout(() => {
          connectSse();
        }, RECONNECT_DELAY_MS);
      };
    };

    connectSse();

    return () => {
      isUnmounted = true;
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
      }
      if (eventSource) {
        eventSource.close();
      }
    };
  }, [queryClient]);

  const queryError =
    servicesQuery.error || alertsQuery.error || dlqQuery.error;

  const errorMessage = queryError
    ? "Consumer service unreachable on :8081 — start backend or Docker stack."
    : null;

  return {
    services: servicesQuery.data ?? [],
    alerts: alertsQuery.data ?? [],
    dlqRecords: dlqQuery.data ?? [],
    timeSeries,
    connectionState,
    lastEventTimestamp,
    eventsPerSecond,
    isLoadingInitial: servicesQuery.isLoading || alertsQuery.isLoading,
    isFetching:
      servicesQuery.isFetching ||
      alertsQuery.isFetching ||
      dlqQuery.isFetching,
    error: errorMessage,
    refreshSnapshot,
  };
}
