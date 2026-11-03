"use client";

import React from "react";
import { Alert, Box, Button, Container, Grid, Group, Stack, Text } from "@mantine/core";
import { AlertCircle } from "lucide-react";
import { useTelemetryStream } from "@/hooks/useTelemetryStream";
import { HeaderBar } from "@/components/HeaderBar";
import { TrafficSimulatorPanel } from "@/components/TrafficSimulatorPanel";
import { LiveTelemetryChart } from "@/components/LiveTelemetryChart";
import { ServiceHealthGrid } from "@/components/ServiceHealthGrid";
import { AnomalyAlertFeed } from "@/components/AnomalyAlertFeed";

export default function SentinelDashboardPage() {
  const {
    services,
    alerts,
    dlqRecords,
    timeSeries,
    connectionState,
    lastEventTimestamp,
    eventsPerSecond,
    isFetching,
    error,
    refreshSnapshot,
  } = useTelemetryStream();

  return (
    <Box component="main" py="lg">
      <Container size="xl">
        <Stack gap="lg">
          <HeaderBar
            services={services}
            alerts={alerts}
            dlqRecords={dlqRecords}
            connectionState={connectionState}
            eventsPerSecond={eventsPerSecond}
            lastEventTimestamp={lastEventTimestamp}
            isFetching={isFetching}
            onRefresh={refreshSnapshot}
          />

          {error && (
            <Alert
              variant="light"
              color="yellow"
              radius="md"
              icon={<AlertCircle size={18} />}
            >
              <Group justify="space-between" wrap="wrap">
                <Text size="xs" ff="monospace">
                  {error}
                </Text>
                <Button
                  size="compact-xs"
                  variant="light"
                  color="yellow"
                  onClick={refreshSnapshot}
                >
                  Retry Queries
                </Button>
              </Group>
            </Alert>
          )}

          {/* Top Row: Live Chart + Traffic & Anomaly Simulator */}
          <Grid gutter="lg" align="stretch">
            <Grid.Col span={{ base: 12, lg: 7 }}>
              <LiveTelemetryChart timeSeries={timeSeries} />
            </Grid.Col>
            <Grid.Col span={{ base: 12, lg: 5 }}>
              <TrafficSimulatorPanel />
            </Grid.Col>
          </Grid>

          {/* Bottom Row: Service Fleet Rolling Window Cards + Live Alert / DLQ Feed */}
          <Grid gutter="lg" align="stretch">
            <Grid.Col span={{ base: 12, lg: 7 }}>
              <ServiceHealthGrid services={services} />
            </Grid.Col>
            <Grid.Col span={{ base: 12, lg: 5 }}>
              <AnomalyAlertFeed alerts={alerts} dlqRecords={dlqRecords} />
            </Grid.Col>
          </Grid>
        </Stack>
      </Container>
    </Box>
  );
}
