# Sentinel Common

Shared library module containing common code used across Sentinel services.

## Contents

- **DTOs**: Data Transfer Objects shared between services
- **Models**: Domain models and entities
- **Protobuf Definitions**: Protocol buffer schemas for serialization
- **Utilities**: Common utility classes and helpers
- **Validators**: Custom validation logic

## Usage

This module is automatically included as a dependency in other Sentinel services.

Add to your service's `pom.xml`:

```xml
<dependency>
    <groupId>com.sentinel</groupId>
    <artifactId>sentinel-common</artifactId>
</dependency>
```
