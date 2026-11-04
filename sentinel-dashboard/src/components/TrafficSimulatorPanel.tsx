"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  Alert,
  Badge,
  Button,
  Card,
  Code,
  Divider,
  Group,
  NumberInput,
  Paper,
  SegmentedControl,
  Select,
  SimpleGrid,
  Stack,
  Text,
  ThemeIcon,
  Title,
  UnstyledButton,
} from "@mantine/core";
import { useMutation } from "@tanstack/react-query";
import {
  Flame,
  Play,
  RotateCcw,
  Send,
  Sliders,
  Square,
  Zap,
} from "lucide-react";
import {
  METRIC_CATALOG,
  MetricRequestPayload,
  SIMULATED_SERVICES,
} from "@/types/telemetry";
import { ingestMetric } from "@/lib/api";

interface TrafficSimulatorPanelProps {
  onMetricSent?: () => void;
}

export function TrafficSimulatorPanel({
  onMetricSent,
}: TrafficSimulatorPanelProps) {
  const [isAutoRunning, setIsAutoRunning] = useState(false);
  const [ratePerSec, setRatePerSec] = useState<string>("2");
  const [sentCount, setSentCount] = useState<number>(0);
  const [lastActionStatus, setLastActionStatus] = useState<{
    type: "ok" | "error";
    text: string;
  } | null>(null);

  // Custom single-metric form state
  const [selectedService, setSelectedService] = useState<string>(
    SIMULATED_SERVICES[0]
  );
  const [selectedMetric, setSelectedMetric] = useState<string>("cpu_usage");
  const [customValue, setCustomValue] = useState<number | string>(64.5);

  const stepIndexRef = useRef(0);

  // TanStack React Query mutation for POST /api/v1/metrics
  const ingestMutation = useMutation({
    mutationFn: ({
      payload,
    }: {
      payload: MetricRequestPayload;
      label?: string;
    }) => ingestMetric(payload),
    onSuccess: (_data, variables) => {
      setSentCount((c) => c + 1);
      setLastActionStatus({
        type: "ok",
        text:
          variables.label ||
          `Sent ${variables.payload.serviceId} -> ${variables.payload.metricName}=${variables.payload.value}`,
      });
      onMetricSent?.();
    },
    onError: (err) => {
      setLastActionStatus({
        type: "error",
        text:
          err instanceof Error
            ? err.message
            : "Failed to reach Ingestion API (:8080)",
      });
    },
  });

  const recoverFleetMutation = useMutation({
    mutationFn: async () => {
      const now = Date.now();
      const payloads: MetricRequestPayload[] = [];
      for (const serviceId of SIMULATED_SERVICES) {
        for (const [metricKey, meta] of Object.entries(METRIC_CATALOG)) {
          const healthyVal =
            Math.round(((meta.normalMin + meta.normalMax) / 2) * 10) / 10;
          payloads.push({
            serviceId,
            metricName: metricKey,
            value: healthyVal,
            unit: meta.unit,
            timestamp: now,
          });
        }
      }
      await Promise.all(payloads.map((p) => ingestMetric(p)));
      return payloads.length;
    },
    onSuccess: (count) => {
      setSentCount((c) => c + count);
      setLastActionStatus({
        type: "ok",
        text: `Normalized all ${SIMULATED_SERVICES.length} services with baseline healthy metrics`,
      });
      onMetricSent?.();
    },
    onError: (err) => {
      setLastActionStatus({
        type: "error",
        text:
          err instanceof Error
            ? err.message
            : "Failed to send fleet recovery batch",
      });
    },
  });

  const sendSinglePayload = useCallback(
    (payload: MetricRequestPayload, label?: string) => {
      ingestMutation.mutate({ payload, label });
    },
    [ingestMutation.mutate]
  );

  // Continuous background load generator
  useEffect(() => {
    if (!isAutoRunning) return;

    const numericRate = Number.parseInt(ratePerSec, 10) || 2;
    const metricKeys = Object.keys(METRIC_CATALOG);
    const intervalMs = Math.max(150, Math.round(1000 / numericRate));

    const timer = setInterval(() => {
      const idx = stepIndexRef.current++;
      const serviceId = SIMULATED_SERVICES[idx % SIMULATED_SERVICES.length];
      const metricKey =
        metricKeys[
          Math.floor(idx / SIMULATED_SERVICES.length) % metricKeys.length
        ];
      const meta = METRIC_CATALOG[metricKey];

      // 88% normal traffic, 12% spike above threshold for realistic live streams
      const isSpike = Math.random() < 0.12;
      const rawValue = isSpike
        ? meta.threshold + (Math.random() * 0.22 + 0.02) * meta.threshold
        : meta.normalMin + Math.random() * (meta.normalMax - meta.normalMin);

      const roundedValue = Math.round(rawValue * 100) / 100;

      sendSinglePayload(
        {
          serviceId,
          metricName: metricKey,
          value: roundedValue,
          unit: meta.unit,
          timestamp: Date.now(),
        },
        `Auto [${serviceId}] ${metricKey}=${roundedValue}${meta.shortUnit}`
      );
    }, intervalMs);

    return () => clearInterval(timer);
  }, [isAutoRunning, ratePerSec, sendSinglePayload]);

  const handleInjectAnomaly = (
    serviceId: string,
    metricName: string,
    value: number,
    scenarioLabel: string
  ) => {
    const meta = METRIC_CATALOG[metricName];
    sendSinglePayload(
      {
        serviceId,
        metricName,
        value,
        unit: meta?.unit || "unit",
        timestamp: Date.now(),
      },
      `Injected ${scenarioLabel}: ${serviceId} ${metricName}=${value}`
    );
  };

  const handleCustomSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const numericValue =
      typeof customValue === "number"
        ? customValue
        : Number.parseFloat(customValue);
    if (Number.isNaN(numericValue)) return;

    const meta = METRIC_CATALOG[selectedMetric];
    sendSinglePayload({
      serviceId: selectedService,
      metricName: selectedMetric,
      value: numericValue,
      unit: meta?.unit || "unit",
      timestamp: Date.now(),
    });
  };

  const isBusy = ingestMutation.isPending || recoverFleetMutation.isPending;

  const chaosButtons = [
    {
      serviceId: "auth-service",
      metricName: "cpu_usage",
      value: 96.4,
      label: "CPU Spike (auth-service)",
      sub: "cpu_usage = 96.4% (Threshold > 90%)",
      severity: "CRITICAL",
      color: "red",
    },
    {
      serviceId: "payment-service",
      metricName: "latency_ms",
      value: 745.0,
      label: "Latency Surge (payment-service)",
      sub: "latency_ms = 745ms (Threshold > 500ms)",
      severity: "CRITICAL",
      color: "red",
    },
    {
      serviceId: "order-service",
      metricName: "memory_usage",
      value: 89.5,
      label: "Heap Pressure (order-service)",
      sub: "memory_usage = 89.5% (Threshold > 85%)",
      severity: "WARNING",
      color: "yellow",
    },
    {
      serviceId: "inventory-service",
      metricName: "error_rate",
      value: 11.4,
      label: "5xx Error Burst (inventory-service)",
      sub: "error_rate = 11.4% (Threshold > 5%)",
      severity: "CRITICAL",
      color: "red",
    },
  ];

  return (
    <Card withBorder padding="lg" radius="md" bg="dark.7">
      <Stack gap="md">
        <Group justify="space-between" align="flex-start" wrap="wrap">
          <Group gap="sm">
            <ThemeIcon size="lg" radius="md" variant="light" color="yellow">
              <Zap size={18} />
            </ThemeIcon>
            <Stack gap={2}>
              <Title order={5}>Traffic &amp; Anomaly Simulator</Title>
              <Text size="xs" c="dimmed">
                Mutates <Code>POST :8080/api/v1/metrics</Code> &rarr; Avro
              </Text>
            </Stack>
          </Group>

          <Group gap="xs">
            <SegmentedControl
              size="xs"
              value={ratePerSec}
              onChange={setRatePerSec}
              data={[
                { label: "1x/s", value: "1" },
                { label: "2x/s", value: "2" },
                { label: "5x/s", value: "5" },
              ]}
            />
            <Button
              size="xs"
              color={isAutoRunning ? "red" : "teal"}
              onClick={() => setIsAutoRunning((v) => !v)}
              leftSection={
                isAutoRunning ? <Square size={13} /> : <Play size={13} />
              }
            >
              {isAutoRunning
                ? `Stop Stream (${sentCount})`
                : "Auto-Simulate Traffic"}
            </Button>
          </Group>
        </Group>

        <Divider />

        <Stack gap="xs">
          <Group justify="space-between">
            <Group gap={6}>
              <Flame size={14} color="var(--mantine-color-red-5)" />
              <Text size="xs" fw={700} tt="uppercase" c="dimmed">
                1-Click Chaos &amp; Threshold Breach Injectors
              </Text>
            </Group>
            <Button
              size="compact-xs"
              variant="subtle"
              color="teal"
              loading={recoverFleetMutation.isPending}
              onClick={() => recoverFleetMutation.mutate()}
              leftSection={<RotateCcw size={12} />}
            >
              Normalize All Services
            </Button>
          </Group>

          <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="xs">
            {chaosButtons.map((btn) => (
              <UnstyledButton
                key={`${btn.serviceId}-${btn.metricName}`}
                disabled={isBusy}
                onClick={() =>
                  handleInjectAnomaly(
                    btn.serviceId,
                    btn.metricName,
                    btn.value,
                    btn.label
                  )
                }
              >
                <Paper
                  withBorder
                  p="sm"
                  radius="md"
                  bg="dark.8"
                  style={{
                    borderColor:
                      btn.color === "red"
                        ? "rgba(250, 82, 82, 0.35)"
                        : "rgba(250, 176, 5, 0.35)",
                  }}
                >
                  <Group justify="space-between" wrap="nowrap">
                    <Stack gap={2}>
                      <Text
                        size="xs"
                        fw={700}
                        c={btn.color === "red" ? "red.3" : "yellow.3"}
                      >
                        {btn.label}
                      </Text>
                      <Text size="xs" c="dimmed" ff="monospace">
                        {btn.sub}
                      </Text>
                    </Stack>
                    <Badge
                      size="xs"
                      variant="light"
                      color={btn.color}
                      ff="monospace"
                    >
                      {btn.severity}
                    </Badge>
                  </Group>
                </Paper>
              </UnstyledButton>
            ))}
          </SimpleGrid>
        </Stack>

        <Divider />

        <form onSubmit={handleCustomSubmit}>
          <Stack gap="xs">
            <Group gap={6}>
              <Sliders size={14} color="var(--mantine-color-indigo-4)" />
              <Text size="xs" fw={700} tt="uppercase" c="dimmed">
                Custom Metric Payload (React Query Mutation)
              </Text>
            </Group>

            <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="xs">
              <Select
                size="xs"
                label="Service ID"
                value={selectedService}
                onChange={(val) => val && setSelectedService(val)}
                data={[...SIMULATED_SERVICES]}
                allowDeselect={false}
              />
              <Select
                size="xs"
                label="Metric Name"
                value={selectedMetric}
                onChange={(val) => {
                  if (!val) return;
                  setSelectedMetric(val);
                  const meta = METRIC_CATALOG[val];
                  if (meta) {
                    setCustomValue(meta.normalMax);
                  }
                }}
                data={Object.values(METRIC_CATALOG).map((meta) => ({
                  value: meta.key,
                  label: `${meta.key} (max ${meta.threshold}${meta.shortUnit})`,
                }))}
                allowDeselect={false}
              />
            </SimpleGrid>

            <Group align="flex-end" grow>
              <NumberInput
                size="xs"
                label="Metric Value"
                step={0.5}
                decimalScale={2}
                value={customValue}
                onChange={setCustomValue}
              />
              <Button
                type="submit"
                size="xs"
                color="indigo"
                loading={ingestMutation.isPending}
                leftSection={<Send size={13} />}
              >
                Send Metric
              </Button>
            </Group>
          </Stack>
        </form>

        {lastActionStatus && (
          <Alert
            variant="light"
            color={lastActionStatus.type === "ok" ? "indigo" : "red"}
            p="xs"
            radius="md"
          >
            <Group justify="space-between" wrap="nowrap">
              <Text size="xs" ff="monospace" truncate>
                {lastActionStatus.text}
              </Text>
              <Badge size="xs" variant="outline" color="gray" ff="monospace">
                Sent: {sentCount}
              </Badge>
            </Group>
          </Alert>
        )}
      </Stack>
    </Card>
  );
}
