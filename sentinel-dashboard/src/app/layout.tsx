import type { Metadata } from "next";
import { ColorSchemeScript } from "@mantine/core";
import "@mantine/core/styles.css";
import "./globals.css";
import { AppProviders } from "@/providers/AppProviders";

export const metadata: Metadata = {
  title: "Sentinel | Real-Time Telemetry & Anomaly Detection",
  description:
    "Distributed microservices telemetry, Apache Avro/Kafka streaming, and real-time SSE anomaly detection dashboard.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" data-mantine-color-scheme="dark">
      <head>
        <ColorSchemeScript defaultColorScheme="dark" />
      </head>
      <body>
        <AppProviders>{children}</AppProviders>
      </body>
    </html>
  );
}
