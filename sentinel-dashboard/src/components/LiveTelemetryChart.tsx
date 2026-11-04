"use client";

import React, { useMemo, useState } from "react";
import {
  Badge,
  Box,
  Card,
  Divider,
  Group,
  Paper,
  SegmentedControl,
  Select,
  Stack,
  Text,
  ThemeIcon,
  Title,
} from "@mantine/core";
import { LineChart, TrendingUp } from "lucide-react";
import {
  METRIC_CATALOG,
  SIMULATED_SERVICES,
  TimeSeriesPoint,
} from "@/types/telemetry";

interface LiveTelemetryChartProps {
  timeSeries: TimeSeriesPoint[];
}

export function LiveTelemetryChart({ timeSeries }: LiveTelemetryChartProps) {
  const [activeMetric, setActiveMetric] = useState<string>("cpu_usage");
  const [selectedService, setSelectedService] = useState<string>("ALL");
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);

  const meta = METRIC_CATALOG[activeMetric] || METRIC_CATALOG.cpu_usage;

  const filteredPoints = useMemo(() => {
    return timeSeries
      .filter(
        (pt) =>
          pt.metricName === activeMetric &&
          (selectedService === "ALL" || pt.serviceId === selectedService)
      )
      .slice(-45);
  }, [timeSeries, activeMetric, selectedService]);

  const width = 760;
  const height = 240;
  const padLeft = 48;
  const padRight = 20;
  const padTop = 22;
  const padBottom = 30;
  const plotW = width - padLeft - padRight;
  const plotH = height - padTop - padBottom;

  const maxDataVal = useMemo(() => {
    const values = filteredPoints.map((p) => p.value);
    const highest = Math.max(meta.threshold * 1.18, ...values, 10);
    return Math.ceil(highest * 1.05);
  }, [filteredPoints, meta.threshold]);

  const toX = React.useCallback(
    (index: number, count: number) => {
      if (count <= 1) return padLeft + plotW / 2;
      return padLeft + (index / (count - 1)) * plotW;
    },
    [padLeft, plotW]
  );

  const toY = React.useCallback(
    (val: number) => {
      const clamped = Math.max(0, Math.min(val, maxDataVal));
      return padTop + plotH - (clamped / maxDataVal) * plotH;
    },
    [maxDataVal, padTop, plotH]
  );

  const thresholdY = toY(meta.threshold);

  const linePath = useMemo(() => {
    if (filteredPoints.length === 0) return "";
    return filteredPoints
      .map(
        (pt, idx) =>
          `${idx === 0 ? "M" : "L"} ${toX(
            idx,
            filteredPoints.length
          ).toFixed(1)} ${toY(pt.value).toFixed(1)}`
      )
      .join(" ");
  }, [filteredPoints, toX, toY]);

  const areaPath = useMemo(() => {
    if (filteredPoints.length < 2) return "";
    const firstX = toX(0, filteredPoints.length).toFixed(1);
    const lastX = toX(
      filteredPoints.length - 1,
      filteredPoints.length
    ).toFixed(1);
    const baseY = (padTop + plotH).toFixed(1);
    return `${linePath} L ${lastX} ${baseY} L ${firstX} ${baseY} Z`;
  }, [filteredPoints.length, linePath, padTop, plotH, toX]);

  const movingAvgPath = useMemo(() => {
    const ptsWithAvg = filteredPoints.filter(
      (pt) => typeof pt.movingAverage === "number"
    );
    if (ptsWithAvg.length < 2) return "";
    return filteredPoints
      .map((pt, idx) => {
        const avgVal = pt.movingAverage ?? pt.value;
        return `${idx === 0 ? "M" : "L"} ${toX(
          idx,
          filteredPoints.length
        ).toFixed(1)} ${toY(avgVal).toFixed(1)}`;
      })
      .join(" ");
  }, [filteredPoints, toX, toY]);

  const hoveredPoint =
    hoveredIndex !== null && filteredPoints[hoveredIndex]
      ? filteredPoints[hoveredIndex]
      : filteredPoints[filteredPoints.length - 1] || null;

  return (
    <Card withBorder padding="lg" radius="md" bg="dark.7">
      <Stack gap="md">
        <Group justify="space-between" align="flex-start" wrap="wrap" gap="sm">
          <Group gap="sm">
            <ThemeIcon size="lg" radius="md" variant="light" color="indigo">
              <LineChart size={18} />
            </ThemeIcon>
            <Stack gap={2}>
              <Title order={5}>Real-Time SSE Telemetry Stream</Title>
              <Text size="xs" c="dimmed">
                {meta.description}
              </Text>
            </Stack>
          </Group>

          <Group gap="xs" wrap="wrap">
            <Select
              size="xs"
              w={160}
              aria-label="Filter Chart by Service"
              value={selectedService}
              onChange={(val) => val && setSelectedService(val)}
              data={[
                { value: "ALL", label: "All Services" },
                ...SIMULATED_SERVICES.map((svc) => ({
                  value: svc,
                  label: svc,
                })),
              ]}
              allowDeselect={false}
            />

            <SegmentedControl
              size="xs"
              value={activeMetric}
              onChange={(val) => {
                setActiveMetric(val);
                setHoveredIndex(null);
              }}
              data={Object.values(METRIC_CATALOG).map((m) => ({
                value: m.key,
                label: m.label,
              }))}
            />
          </Group>
        </Group>

        <Divider />

        <Paper withBorder p="xs" radius="md" bg="dark.8">
          <Group justify="space-between" wrap="wrap" gap="xs">
            <Group gap="md">
              <Text size="xs" ff="monospace" c="dimmed">
                Threshold:{" "}
                <Text span fw={700} c="red.4">
                  {meta.threshold}
                  {meta.shortUnit}
                </Text>
              </Text>
              <Text size="xs" ff="monospace" c="dimmed">
                Window Points:{" "}
                <Text span fw={700} c="gray.2">
                  {filteredPoints.length}
                </Text>
              </Text>
            </Group>

            {hoveredPoint ? (
              <Group gap="sm">
                <Badge size="xs" variant="light" color="indigo" ff="monospace">
                  {hoveredPoint.serviceId}
                </Badge>
                <Text size="xs" ff="monospace">
                  Value:{" "}
                  <Text
                    span
                    fw={700}
                    c={hoveredPoint.isAnomaly ? "red.4" : "teal.4"}
                  >
                    {hoveredPoint.value}
                    {meta.shortUnit}
                  </Text>
                </Text>
                {hoveredPoint.movingAverage !== undefined && (
                  <Text size="xs" ff="monospace" c="dimmed">
                    Avg:{" "}
                    <Text span fw={700} c="blue.3">
                      {hoveredPoint.movingAverage}
                      {meta.shortUnit}
                    </Text>
                  </Text>
                )}
                <Text size="xs" ff="monospace" c="dimmed">
                  {new Date(hoveredPoint.timestamp).toLocaleTimeString()}
                </Text>
              </Group>
            ) : (
              <Text size="xs" ff="monospace" c="dimmed">
                Click &ldquo;Auto-Simulate Traffic&rdquo; or inject a spike to
                stream points
              </Text>
            )}
          </Group>
        </Paper>

        <Paper
          withBorder
          p="xs"
          radius="md"
          bg="dark.9"
          pos="relative"
          style={{ overflow: "hidden" }}
        >
          <svg
            viewBox={`0 0 ${width} ${height}`}
            style={{
              width: "100%",
              height: 224,
              userSelect: "none",
              display: "block",
            }}
            role="img"
            aria-label={`Live ${meta.label} telemetry chart`}
          >
            <defs>
              <linearGradient
                id="telemetryAreaGrad"
                x1="0"
                y1="0"
                x2="0"
                y2="1"
              >
                <stop offset="0%" stopColor="#6366f1" stopOpacity="0.35" />
                <stop offset="100%" stopColor="#6366f1" stopOpacity="0.0" />
              </linearGradient>
            </defs>

            {[0, 0.25, 0.5, 0.75, 1].map((ratio) => {
              const val = Math.round(maxDataVal * ratio * 10) / 10;
              const y = toY(val);
              return (
                <g key={ratio}>
                  <line
                    x1={padLeft}
                    y1={y}
                    x2={width - padRight}
                    y2={y}
                    stroke="#25262b"
                    strokeDasharray="3 3"
                  />
                  <text
                    x={padLeft - 8}
                    y={y + 4}
                    textAnchor="end"
                    fill="#909296"
                    fontSize="10"
                    fontFamily="monospace"
                  >
                    {val}
                    {meta.shortUnit}
                  </text>
                </g>
              );
            })}

            <line
              x1={padLeft}
              y1={thresholdY}
              x2={width - padRight}
              y2={thresholdY}
              stroke="#fa5252"
              strokeWidth="1.5"
              strokeDasharray="6 4"
            />
            <text
              x={width - padRight - 4}
              y={Math.max(12, thresholdY - 6)}
              textAnchor="end"
              fill="#ff8787"
              fontSize="10"
              fontFamily="monospace"
              fontWeight="bold"
            >
              Anomaly Threshold ({meta.threshold}
              {meta.shortUnit})
            </text>

            {areaPath && <path d={areaPath} fill="url(#telemetryAreaGrad)" />}

            {movingAvgPath && (
              <path
                d={movingAvgPath}
                fill="none"
                stroke="#4dabf7"
                strokeWidth="1.5"
                strokeDasharray="4 3"
              />
            )}

            {linePath && (
              <path
                d={linePath}
                fill="none"
                stroke="#748ffc"
                strokeWidth="2.25"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            )}

            {filteredPoints.map((pt, idx) => {
              const cx = toX(idx, filteredPoints.length);
              const cy = toY(pt.value);
              return (
                <g
                  key={pt.id}
                  onMouseEnter={() => setHoveredIndex(idx)}
                  onMouseLeave={() => setHoveredIndex(null)}
                  style={{ cursor: "pointer" }}
                >
                  <circle cx={cx} cy={cy} r={10} fill="transparent" />
                  {pt.isAnomaly && (
                    <circle
                      cx={cx}
                      cy={cy}
                      r={7}
                      fill="#fa5252"
                      fillOpacity={0.25}
                    />
                  )}
                  <circle
                    cx={cx}
                    cy={cy}
                    r={pt.isAnomaly ? 4.5 : 3}
                    fill={pt.isAnomaly ? "#fa5252" : "#91a7ff"}
                    stroke="#141517"
                    strokeWidth="1.5"
                  />
                </g>
              );
            })}
          </svg>

          {filteredPoints.length === 0 && (
            <Box
              pos="absolute"
              inset={0}
              style={{
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                pointerEvents: "none",
              }}
            >
              <TrendingUp size={24} color="var(--mantine-color-dark-3)" />
              <Text size="xs" c="dimmed" ff="monospace" mt={6}>
                No live points recorded for {meta.key} yet
              </Text>
            </Box>
          )}
        </Paper>

        <Group gap="lg" wrap="wrap">
          <Badge size="xs" variant="dot" color="indigo" ff="monospace">
            Raw Metric Value
          </Badge>
          <Badge size="xs" variant="dot" color="blue" ff="monospace">
            Rolling Window Moving Average
          </Badge>
          <Badge size="xs" variant="dot" color="red" ff="monospace">
            Threshold Breach (&gt; {meta.threshold}
            {meta.shortUnit})
          </Badge>
        </Group>
      </Stack>
    </Card>
  );
}
