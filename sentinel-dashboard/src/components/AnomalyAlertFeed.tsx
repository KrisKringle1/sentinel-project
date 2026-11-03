"use client";

import React, { useMemo, useState } from "react";
import {
  Badge,
  Card,
  Code,
  Group,
  Paper,
  ScrollArea,
  SegmentedControl,
  Stack,
  Tabs,
  Text,
  ThemeIcon,
} from "@mantine/core";
import {
  AlertOctagon,
  AlertTriangle,
  Bell,
  Inbox,
  ShieldCheck,
} from "lucide-react";
import {
  AnomalyAlert,
  DlqMetricRecord,
  METRIC_CATALOG,
} from "@/types/telemetry";

interface AnomalyAlertFeedProps {
  alerts: AnomalyAlert[];
  dlqRecords: DlqMetricRecord[];
}

export function AnomalyAlertFeed({
  alerts,
  dlqRecords,
}: AnomalyAlertFeedProps) {
  const [severityFilter, setSeverityFilter] = useState<string>("ALL");

  const filteredAlerts = useMemo(() => {
    if (severityFilter === "ALL") return alerts;
    return alerts.filter((a) => a.severity === severityFilter);
  }, [alerts, severityFilter]);

  return (
    <Card withBorder padding="lg" radius="md" bg="dark.7">
      <Tabs defaultValue="alerts" variant="pills" radius="md">
        <Group justify="space-between" mb="md" wrap="wrap" gap="xs">
          <Tabs.List>
            <Tabs.Tab
              value="alerts"
              color="red"
              leftSection={<Bell size={14} />}
            >
              Anomaly Alerts ({alerts.length})
            </Tabs.Tab>
            <Tabs.Tab
              value="dlq"
              color="yellow"
              leftSection={<Inbox size={14} />}
            >
              DLQ Inspector ({dlqRecords.length})
            </Tabs.Tab>
          </Tabs.List>

          <SegmentedControl
            size="xs"
            value={severityFilter}
            onChange={setSeverityFilter}
            data={[
              { label: "ALL", value: "ALL" },
              { label: "CRITICAL", value: "CRITICAL" },
              { label: "WARNING", value: "WARNING" },
            ]}
          />
        </Group>

        <Tabs.Panel value="alerts">
          <ScrollArea.Autosize mah={540} offsetScrollbars>
            {filteredAlerts.length === 0 ? (
              <Stack align="center" gap="xs" py="xl">
                <ThemeIcon size="xl" radius="xl" variant="light" color="teal">
                  <ShieldCheck size={22} />
                </ThemeIcon>
                <Text size="sm" fw={600}>
                  No anomaly threshold breaches recorded
                </Text>
                <Text size="xs" c="dimmed" ff="monospace" ta="center">
                  Trigger a 1-Click Chaos Injector to test real-time alerting.
                </Text>
              </Stack>
            ) : (
              <Stack gap="xs">
                {filteredAlerts.map((alert) => {
                  const isCrit = alert.severity === "CRITICAL";
                  const unit =
                    METRIC_CATALOG[alert.metricName]?.shortUnit || "";
                  return (
                    <Paper
                      key={
                        alert.alertId ||
                        `${alert.serviceId}-${alert.metricName}-${alert.timestamp}`
                      }
                      withBorder
                      p="sm"
                      radius="md"
                      bg="dark.8"
                      style={{
                        borderColor: isCrit
                          ? "rgba(250, 82, 82, 0.4)"
                          : "rgba(250, 176, 5, 0.35)",
                      }}
                    >
                      <Stack gap={6}>
                        <Group justify="space-between" wrap="nowrap">
                          <Group gap="xs">
                            <ThemeIcon
                              size="sm"
                              radius="sm"
                              variant="light"
                              color={isCrit ? "red" : "yellow"}
                            >
                              {isCrit ? (
                                <AlertOctagon size={13} />
                              ) : (
                                <AlertTriangle size={13} />
                              )}
                            </ThemeIcon>
                            <Text size="xs" fw={700} ff="monospace">
                              {alert.serviceId}
                            </Text>
                            <Badge
                              size="xs"
                              variant="light"
                              color={isCrit ? "red" : "yellow"}
                              ff="monospace"
                            >
                              {alert.severity}
                            </Badge>
                          </Group>
                          <Text size="xs" c="dimmed" ff="monospace">
                            {new Date(alert.timestamp).toLocaleTimeString()}
                          </Text>
                        </Group>

                        <Text size="xs" ff="monospace">
                          {alert.message}
                        </Text>

                        <Group gap="md">
                          <Text size="xs" c="dimmed" ff="monospace">
                            Metric:{" "}
                            <Text span fw={700} c="gray.2">
                              {alert.metricName}
                            </Text>
                          </Text>
                          <Text size="xs" c="dimmed" ff="monospace">
                            Observed:{" "}
                            <Text
                              span
                              fw={700}
                              c={isCrit ? "red.3" : "yellow.3"}
                            >
                              {alert.value.toFixed(2)}
                              {unit}
                            </Text>
                          </Text>
                          <Text size="xs" c="dimmed" ff="monospace">
                            Threshold:{" "}
                            <Text span fw={700} c="gray.3">
                              {alert.threshold.toFixed(2)}
                              {unit}
                            </Text>
                          </Text>
                        </Group>
                      </Stack>
                    </Paper>
                  );
                })}
              </Stack>
            )}
          </ScrollArea.Autosize>
        </Tabs.Panel>

        <Tabs.Panel value="dlq">
          <ScrollArea.Autosize mah={540} offsetScrollbars>
            {dlqRecords.length === 0 ? (
              <Stack align="center" gap="xs" py="xl">
                <ThemeIcon size="xl" radius="xl" variant="light" color="gray">
                  <Inbox size={22} />
                </ThemeIcon>
                <Text size="sm" fw={600}>
                  Dead-Letter Queue is empty
                </Text>
                <Text size="xs" c="dimmed" ff="monospace" ta="center">
                  Failed primary Kafka sends are routed to{" "}
                  <Code>service-metrics-dlq</Code> and captured here.
                </Text>
              </Stack>
            ) : (
              <Stack gap="xs">
                {dlqRecords.map((rec, idx) => (
                  <Paper
                    key={`${rec.serviceId}-${rec.metricName}-${rec.receivedAtTimestamp}-${idx}`}
                    withBorder
                    p="sm"
                    radius="md"
                    bg="dark.8"
                  >
                    <Group justify="space-between">
                      <Text size="xs" fw={700} c="yellow.4" ff="monospace">
                        {rec.serviceId} &bull; {rec.metricName}
                      </Text>
                      <Text size="xs" c="dimmed" ff="monospace">
                        {new Date(rec.receivedAtTimestamp).toLocaleTimeString()}
                      </Text>
                    </Group>
                    <Text size="xs" ff="monospace" mt={4}>
                      Value: <strong>{rec.value}</strong> (Metric timestamp:{" "}
                      {new Date(rec.metricTimestamp).toLocaleTimeString()})
                    </Text>
                  </Paper>
                ))}
              </Stack>
            )}
          </ScrollArea.Autosize>
        </Tabs.Panel>
      </Tabs>
    </Card>
  );
}
