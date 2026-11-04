"use client";

import React from "react";
import {
  Badge,
  Card,
  Code,
  Divider,
  Group,
  Paper,
  Progress,
  SimpleGrid,
  Stack,
  Text,
  ThemeIcon,
  Title,
} from "@mantine/core";
import {
  Activity,
  AlertOctagon,
  AlertTriangle,
  CheckCircle,
  Clock,
  Cpu,
} from "lucide-react";
import {
  METRIC_CATALOG,
  MetricSummary,
  ServiceTelemetrySummary,
} from "@/types/telemetry";

interface ServiceHealthGridProps {
  services: ServiceTelemetrySummary[];
}

export function ServiceHealthGrid({ services }: ServiceHealthGridProps) {
  return (
    <Stack gap="sm">
      <Group justify="space-between" wrap="wrap">
        <Group gap="xs">
          <ThemeIcon size="md" radius="md" variant="light" color="indigo">
            <Cpu size={16} />
          </ThemeIcon>
          <Title order={5}>
            Microservice Fleet Rolling Window Aggregations
          </Title>
        </Group>
        <Text size="xs" c="dimmed" ff="monospace">
          Backed by Redis Hash <Code>sentinel:telemetry:services</Code>
        </Text>
      </Group>

      {services.length === 0 ? (
        <Card withBorder padding="xl" radius="md" bg="dark.7">
          <Stack align="center" gap="xs" py="md">
            <ThemeIcon size="xl" radius="xl" variant="light" color="gray">
              <Activity size={22} />
            </ThemeIcon>
            <Text size="sm" fw={600}>
              No services have reported telemetry yet
            </Text>
            <Text size="xs" c="dimmed" ff="monospace" ta="center">
              Use the Traffic &amp; Anomaly Simulator to stream metrics into
              Kafka topic <Code>service-metrics</Code>.
            </Text>
          </Stack>
        </Card>
      ) : (
        <SimpleGrid cols={{ base: 1, md: 2 }} spacing="md">
          {services.map((service) => {
            const statusColor =
              service.status === "CRITICAL"
                ? "red"
                : service.status === "WARNING"
                ? "yellow"
                : "teal";

            const statusIcon =
              service.status === "CRITICAL" ? (
                <AlertOctagon size={13} />
              ) : service.status === "WARNING" ? (
                <AlertTriangle size={13} />
              ) : (
                <CheckCircle size={13} />
              );

            const metricsEntries = Object.values(service.metrics || {});

            return (
              <Card
                key={service.serviceId}
                withBorder
                padding="lg"
                radius="md"
                bg="dark.7"
                style={{
                  borderColor:
                    service.status === "CRITICAL"
                      ? "rgba(250, 82, 82, 0.45)"
                      : service.status === "WARNING"
                      ? "rgba(250, 176, 5, 0.45)"
                      : undefined,
                }}
              >
                <Stack gap="md">
                  <Group justify="space-between" align="flex-start">
                    <Stack gap={2}>
                      <Title order={5} ff="monospace">
                        {service.serviceId}
                      </Title>
                      <Group gap="xs">
                        <Text size="xs" c="dimmed" ff="monospace">
                          Events:{" "}
                          <Text span fw={700} c="gray.2">
                            {service.totalEvents}
                          </Text>
                        </Text>
                        <Text size="xs" c="dimmed">
                          &bull;
                        </Text>
                        <Text size="xs" c="dimmed" ff="monospace">
                          Anomalies:{" "}
                          <Text
                            span
                            fw={700}
                            c={service.anomalyCount > 0 ? "red.4" : "gray.2"}
                          >
                            {service.anomalyCount}
                          </Text>
                        </Text>
                      </Group>
                    </Stack>

                    <Badge
                      variant="light"
                      color={statusColor}
                      radius="sm"
                      ff="monospace"
                      leftSection={statusIcon}
                    >
                      {service.status}
                    </Badge>
                  </Group>

                  <Divider />

                  <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="sm">
                    {metricsEntries.map((metric: MetricSummary) => {
                      const meta = METRIC_CATALOG[metric.metricName];
                      const unit = meta?.shortUnit || "";
                      const threshold = meta?.threshold;
                      const isBreached =
                        threshold !== undefined && metric.lastValue > threshold;
                      const ratio = threshold
                        ? Math.min(
                            100,
                            Math.round((metric.lastValue / threshold) * 100)
                          )
                        : 50;

                      return (
                        <Paper
                          key={metric.metricName}
                          withBorder
                          p="sm"
                          radius="md"
                          bg="dark.8"
                          style={{
                            borderColor: isBreached
                              ? "rgba(250, 82, 82, 0.4)"
                              : undefined,
                          }}
                        >
                          <Stack gap={6}>
                            <Group justify="space-between">
                              <Text size="xs" fw={600} ff="monospace">
                                {meta?.label || metric.metricName}
                              </Text>
                              <Text
                                size="sm"
                                fw={700}
                                ff="monospace"
                                c={isBreached ? "red.4" : "teal.4"}
                              >
                                {metric.lastValue.toFixed(1)}
                                {unit}
                              </Text>
                            </Group>

                            <Progress
                              size="sm"
                              radius="xl"
                              value={ratio}
                              color={
                                isBreached
                                  ? "red"
                                  : ratio > 80
                                  ? "yellow"
                                  : "indigo"
                              }
                            />

                            <SimpleGrid cols={3} spacing={4} pt={2}>
                              <Stack gap={0}>
                                <Text size="10px" c="dimmed" ff="monospace">
                                  Avg
                                </Text>
                                <Text size="xs" c="blue.3" ff="monospace">
                                  {metric.movingAverage.toFixed(1)}
                                  {unit}
                                </Text>
                              </Stack>
                              <Stack gap={0}>
                                <Text size="10px" c="dimmed" ff="monospace">
                                  Min
                                </Text>
                                <Text size="xs" ff="monospace">
                                  {metric.min.toFixed(1)}
                                  {unit}
                                </Text>
                              </Stack>
                              <Stack gap={0}>
                                <Text size="10px" c="dimmed" ff="monospace">
                                  Max
                                </Text>
                                <Text size="xs" ff="monospace">
                                  {metric.max.toFixed(1)}
                                  {unit}
                                </Text>
                              </Stack>
                            </SimpleGrid>
                          </Stack>
                        </Paper>
                      );
                    })}
                  </SimpleGrid>

                  <Group justify="space-between" pt={2}>
                    <Group gap={4}>
                      <Clock size={12} color="var(--mantine-color-dimmed)" />
                      <Text size="xs" c="dimmed" ff="monospace">
                        Last updated:{" "}
                        {service.lastSeenTimestamp
                          ? new Date(
                              service.lastSeenTimestamp
                            ).toLocaleTimeString()
                          : "N/A"}
                      </Text>
                    </Group>
                    <Badge
                      size="xs"
                      variant="outline"
                      color="gray"
                      ff="monospace"
                    >
                      Window n=50
                    </Badge>
                  </Group>
                </Stack>
              </Card>
            );
          })}
        </SimpleGrid>
      )}
    </Stack>
  );
}
