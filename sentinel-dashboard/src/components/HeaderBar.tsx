"use client";

import React from "react";
import {
  Badge,
  Button,
  Card,
  Group,
  Paper,
  SimpleGrid,
  Stack,
  Text,
  ThemeIcon,
  Title,
} from "@mantine/core";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Database,
  Radio,
  RefreshCw,
  Server,
  ShieldAlert,
} from "lucide-react";
import {
  AnomalyAlert,
  DlqMetricRecord,
  ServiceTelemetrySummary,
  StreamConnectionState,
} from "@/types/telemetry";

interface HeaderBarProps {
  services: ServiceTelemetrySummary[];
  alerts: AnomalyAlert[];
  dlqRecords: DlqMetricRecord[];
  connectionState: StreamConnectionState;
  eventsPerSecond: number;
  lastEventTimestamp: number | null;
  isFetching?: boolean;
  onRefresh: () => void;
}

export function HeaderBar({
  services,
  alerts,
  dlqRecords,
  connectionState,
  eventsPerSecond,
  lastEventTimestamp,
  isFetching = false,
  onRefresh,
}: HeaderBarProps) {
  const totalEvents = services.reduce((acc, s) => acc + s.totalEvents, 0);
  const healthyCount = services.filter((s) => s.status === "HEALTHY").length;
  const warningCount = services.filter((s) => s.status === "WARNING").length;
  const criticalCount = services.filter((s) => s.status === "CRITICAL").length;
  const criticalAlerts = alerts.filter((a) => a.severity === "CRITICAL").length;

  const statusColor =
    connectionState === "CONNECTED"
      ? "teal"
      : connectionState === "RECONNECTING"
      ? "yellow"
      : "red";

  return (
    <Stack gap="md">
      <Paper withBorder p="lg" radius="md" bg="dark.7">
        <Group justify="space-between" align="center" wrap="wrap" gap="md">
          <Stack gap={4}>
            <Group gap="sm">
              <ThemeIcon size="lg" radius="md" variant="light" color="indigo">
                <Activity size={18} />
              </ThemeIcon>
              <Title order={3} fw={800} lts={-0.3}>
                SENTINEL
              </Title>
              <Badge variant="light" color="indigo" radius="sm" ff="monospace">
                Telemetry &amp; Anomaly Command Center
              </Badge>
            </Group>
            <Text size="xs" c="dimmed" ff="monospace">
              Pipeline: HTTP :8080 &rarr; Avro (Schema Registry :8085) &rarr;
              Kafka KRaft :9092 &rarr; Consumer + Redis :8081 &rarr; SSE Stream
            </Text>
          </Stack>

          <Group gap="sm" wrap="wrap">
            <Badge
              size="lg"
              variant="default"
              radius="md"
              ff="monospace"
              leftSection={<Radio size={14} />}
            >
              Throughput: {eventsPerSecond.toFixed(1)} evt/s
            </Badge>

            <Badge
              size="lg"
              variant="dot"
              color={statusColor}
              radius="md"
              ff="monospace"
            >
              SSE: {connectionState}
            </Badge>

            <Button
              size="xs"
              variant="light"
              color="gray"
              onClick={onRefresh}
              loading={isFetching}
              leftSection={<RefreshCw size={14} />}
            >
              Sync Query Cache
            </Button>
          </Group>
        </Group>
      </Paper>

      <SimpleGrid cols={{ base: 1, sm: 2, lg: 4 }} spacing="md">
        <Card withBorder padding="md" radius="md" bg="dark.7">
          <Group justify="space-between" align="flex-start">
            <Stack gap={4}>
              <Text size="xs" c="dimmed" tt="uppercase" fw={700}>
                Active Fleet Services
              </Text>
              <Title order={2} ff="monospace">
                {services.length}
              </Title>
              <Group gap={6} mt={4}>
                <Badge size="xs" variant="light" color="teal" ff="monospace">
                  {healthyCount} Healthy
                </Badge>
                <Badge size="xs" variant="light" color="yellow" ff="monospace">
                  {warningCount} Warn
                </Badge>
                <Badge size="xs" variant="light" color="red" ff="monospace">
                  {criticalCount} Crit
                </Badge>
              </Group>
            </Stack>
            <ThemeIcon size="lg" radius="md" variant="light" color="indigo">
              <Server size={18} />
            </ThemeIcon>
          </Group>
        </Card>

        <Card withBorder padding="md" radius="md" bg="dark.7">
          <Group justify="space-between" align="flex-start">
            <Stack gap={4}>
              <Text size="xs" c="dimmed" tt="uppercase" fw={700}>
                Total Avro Events Processed
              </Text>
              <Title order={2} ff="monospace">
                {totalEvents.toLocaleString()}
              </Title>
              <Text size="xs" c="dimmed" ff="monospace" mt={4}>
                {lastEventTimestamp
                  ? `Last event ${new Date(
                      lastEventTimestamp
                    ).toLocaleTimeString()}`
                  : "Awaiting telemetry stream..."}
              </Text>
            </Stack>
            <ThemeIcon size="lg" radius="md" variant="light" color="teal">
              <CheckCircle2 size={18} />
            </ThemeIcon>
          </Group>
        </Card>

        <Card withBorder padding="md" radius="md" bg="dark.7">
          <Group justify="space-between" align="flex-start">
            <Stack gap={4}>
              <Text size="xs" c="dimmed" tt="uppercase" fw={700}>
                Recent Anomaly Alerts
              </Text>
              <Title order={2} ff="monospace">
                {alerts.length}
              </Title>
              <Text size="xs" c="red.4" ff="monospace" mt={4}>
                {criticalAlerts} CRITICAL / {alerts.length - criticalAlerts}{" "}
                WARNING
              </Text>
            </Stack>
            <ThemeIcon size="lg" radius="md" variant="light" color="red">
              <ShieldAlert size={18} />
            </ThemeIcon>
          </Group>
        </Card>

        <Card withBorder padding="md" radius="md" bg="dark.7">
          <Group justify="space-between" align="flex-start">
            <Stack gap={4}>
              <Text size="xs" c="dimmed" tt="uppercase" fw={700}>
                Dead-Letter Queue (DLQ)
              </Text>
              <Title order={2} ff="monospace">
                {dlqRecords.length}
              </Title>
              <Text size="xs" c="dimmed" ff="monospace" mt={4}>
                Topic: service-metrics-dlq
              </Text>
            </Stack>
            <ThemeIcon size="lg" radius="md" variant="light" color="yellow">
              {dlqRecords.length > 0 ? (
                <AlertTriangle size={18} />
              ) : (
                <Database size={18} />
              )}
            </ThemeIcon>
          </Group>
        </Card>
      </SimpleGrid>
    </Stack>
  );
}
