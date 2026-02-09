# Deployment & Operations

## Purpose

Documents how to build, deploy, and operate a RowStore instance, including Spring Boot standalone deployment, Docker, CI/CD, PostgreSQL requirements, monitoring, logging, and shutdown behavior.

## Scope

Covers operational concerns. For configuration options, see [06-configuration.md](06-configuration.md). For the system architecture being deployed, see [01-system-architecture.md](01-system-architecture.md).

## DEPL-1 Spring Boot Standalone

> **DEPL-1.01** Build the executable JAR:
> ```bash
> mvn -Dmaven.test.skip=true install
> ```
> This produces `target/rowstore-<version>.jar`.

> **DEPL-1.02** Run the server:
> ```bash
> java -jar target/rowstore-2.0-SNAPSHOT.jar --rowstore.config.uri=file:///path/to/rowstore.json
> ```
> The config URI is required (or set `ROWSTORE_CONFIG_URI`). Port defaults to 8282.

> **DEPL-1.03** Standard Spring Boot properties can be used:
> - `--server.port=8282` — Listen port
> - `--rowstore.config.uri=<URI>` — Configuration file
> - `--spring.main.banner-mode=off` — Disable startup banner
>
> See [CFG-7](06-configuration.md#cfg-7-cli-options) for details.

> **DEPL-1.04** [REMOVED] HTTP/2 and `useForwardedForHeader` — These were Jetty-specific. For reverse proxy support with Tomcat, use Spring Boot's `server.forward-headers-strategy=native` property.

## DEPL-2 Docker

> **DEPL-2.01** Official Docker image: `metasolutions/rowstore:<version>` on Docker Hub.

> **DEPL-2.02** The Dockerfile is maintained **in the RowStore repository** (root `Dockerfile`). It uses `eclipse-temurin:25-jre-alpine` as the base image and copies the pre-built JAR.

> **DEPL-2.03** Docker tags follow these conventions:
> - Version-specific: `2.0`, `2.0-SNAPSHOT`
> - Major.minor shorthand
> - `develop` — Latest development build

## DEPL-3 CI/CD

> **DEPL-3.01** CI/CD uses Bitbucket Pipelines with this workflow:
> ```
> Build (mvn install, skip ITs)
>   → Integration Test (Testcontainers PostgreSQL)
>     → Deploy (artifacts + signatures) + Docker Hub push
> ```

> **DEPL-3.02** Branch strategy:
> - **master**: Full pipeline — build, test, deploy, Docker push (release tags)
> - **develop**: Full pipeline — build, test, deploy, Docker push (`develop` tag)
> - **Other branches**: Build and test only (no deploy, no Docker push)

> **DEPL-3.03** Deploy artifacts:
> - Executable JAR (`.jar`)
> - SHA256 checksum
> - GPG signature
> - Uploaded via SCP to metasolutions.se

## DEPL-4 PostgreSQL Requirements

> **DEPL-4.01** PostgreSQL 9.4+ is required for JSONB column support. Recommended: PostgreSQL 16 (used in CI/CD tests).

> **DEPL-4.02** Schema is managed by **Flyway** with `baselineOnMigrate=true`. On first startup against an existing database, Flyway adopts the current schema. New migrations are applied automatically. The initial migration (`V1__initial_schema.sql`) creates the `datasets` and `aliases` tables.

## DEPL-5 Monitoring

> **DEPL-5.01** `GET /status` returns:
> ```json
> {
>     "service": "RowStore",
>     "version": "2.0-SNAPSHOT",
>     "datasets": 42,
>     "activeEtlProcesses": 2
> }
> ```

> **DEPL-5.02** `GET /status?jvm` adds JVM details:
> ```json
> {
>     "jvm": {
>         "totalMemory": 268435456,
>         "freeMemory": 134217728,
>         "maxMemory": 536870912,
>         "availableProcessors": 4,
>         "totalCommittedMemory": 268435456,
>         "committedHeap": 268435456,
>         "totalUsedMemory": 134217728,
>         "usedHeap": 134217728
>     }
> }
> ```
> Memory values are in bytes. Data sourced from `Runtime` and `ManagementFactory.getMemoryMXBean()`.

> **DEPL-5.03** **Spring Boot Actuator** endpoints are available:
> - `/actuator/health` — Application health with database connectivity check
> - `/actuator/metrics` — JVM, HTTP, and HikariCP connection pool metrics
> - `/actuator/info` — Application information
>
> A custom `RowStoreHealthIndicator` reports dataset count and active ETL processes.

## DEPL-6 Logging

> **DEPL-6.01** Logging uses Log4j2 (via SLF4J). Output is console-only (no file appenders by default). Log level can be set via the `loglevel` config option.

## DEPL-7 Shutdown

> **DEPL-7.01** Graceful shutdown is handled by `EtlProcessor.shutdown()`:
> - Calls `ExecutorService.shutdown()` to stop accepting new tasks
> - Awaits termination of active virtual threads (30-second timeout)
> - Active ETL processing completes or is interrupted after the timeout

## Known Limitations

- No rolling restart or zero-downtime deployment support
- Shutdown may interrupt active ETL jobs if they exceed the 30-second grace period

## References

- [System Architecture](01-system-architecture.md#arch-4-module-structure) — System overview
- [Configuration](06-configuration.md#cfg-7-cli-options) — All configuration options
- [Security](07-security.md#sec-5-rate-limiting) — Security considerations for deployment
- [Glossary](12-glossary.md#glo-1-terms) — Term definitions
- Source: `RowStoreApplication.java`, `DataSourceConfig.java`, `RowStoreHealthIndicator.java`, `bitbucket-pipelines.yml`, `Dockerfile`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: executable JAR, in-repo Dockerfile, Flyway, Actuator, graceful shutdown |
