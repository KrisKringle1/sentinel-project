# Sentinel Common

Shared library module containing common Avro schemas and generated data models used across Sentinel services.

## Contents

- **Apache Avro Schema (`src/main/avro/sentinel.avsc`)**: Defines `com.sentinel.common.avro.SentinelMetric` (`serviceId`,
  `metricName`, `value`, `timestamp`).
- **Generated Avro Classes**: Automatically generated during the Maven `generate-sources` phase via `avro-maven-plugin`.

## Usage

Add `sentinel-common` to a service's `pom.xml`:

```xml
<dependency>
    <groupId>com.sentinel</groupId>
    <artifactId>sentinel-common</artifactId>
</dependency>
```
