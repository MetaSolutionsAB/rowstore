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
> The config URI (or a file path, CFG-2.02) is required, or set `ROWSTORE_CONFIG_URI`. Port defaults to 8282. Arguments that are not of the form `--name=value`, including the options of 1.x, prevent the startup ([CFG-7.05](06-configuration.md#cfg-7-cli-options)).

> **DEPL-1.03** Standard Spring Boot properties can be used:
> - `--server.port=8282` — Listen port
> - `--rowstore.config.uri=<URI>` — Configuration file
> - `--spring.main.banner-mode=off` — Disable startup banner
>
> See [CFG-7](06-configuration.md#cfg-7-cli-options) for details.

> **DEPL-1.04** [REMOVED] HTTP/2 and `useForwardedForHeader` — These were Jetty-specific. `X-Forwarded-For` from reverse proxies is evaluated according to `trustedproxies` ([CFG-3.11](06-configuration.md#cfg-3-application-options)); `server.forward-headers-strategy` is set to `none` and must not be changed, since Tomcat's `RemoteIpValve` would bypass `trustedproxies`.

> **DEPL-1.05** RowStore must be deployed behind a reverse proxy that fulfills the contract in [SEC-9](07-security.md#sec-9-reverse-proxy-contract): authentication of non-GET/HEAD requests, blocking of `/actuator/**`, Swagger UI, OpenAPI description and `/status?jvm`, and setting `X-Forwarded-For`. The [README Security section](../README.md#security) contains a minimal nginx example.

## DEPL-2 Docker

> **DEPL-2.01** Official Docker image: `metasolutions/rowstore:<version>` on Docker Hub.

> **DEPL-2.02** The Dockerfile is maintained **in the RowStore repository** (root `Dockerfile`). It uses `eclipse-temurin:25-jre-alpine` as the base image and copies the pre-built JAR.

> **DEPL-2.04** The image runs RowStore as the unprivileged user `rowstore` (UID and GID 10001), with the working directory `/srv/rowstore`. The configuration location is set as the environment variable `ROWSTORE_CONFIG_URI=file:///srv/rowstore/rowstore.json`, so arguments of `docker run` are passed to RowStore without replacing it. The entrypoint runs `exec java -XX:MaxRAMPercentage=75 $JAVA_OPTS -jar rowstore.jar "$@"`: JVM options are passed in `JAVA_OPTS` (as with the 1.x start script) and take precedence over the default heap of 75% of the container's memory limit, and java replaces the shell, so it receives the stop signal.

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
>     "activeEtlProcesses": 2,
>     "queuedEtlProcesses": 0,
>     "connections": {
>         "database": {
>             "query": {"inUse": 1, "max": 10, "waiting": 0},
>             "export": {"inUse": 0, "max": 2, "waiting": 0},
>             "load": {"inUse": 0, "max": 5, "waiting": 0}
>         }
>     }
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
>
> `/status?jvm` and the Actuator endpoints are served on the application port and must not be publicly reachable ([SEC-9.03](07-security.md#sec-9-reverse-proxy-contract)).

> **DEPL-5.04** While the database is not reachable at startup, RowStore waits for it and does not listen on its port ([ARCH-7.08](01-system-architecture.md#arch-7-database-connection-limits)): health checks fail to connect until the database is reachable, and an orchestrator should not restart the instance only because it does not answer yet during that time (e.g., use a generous start period). Responses with 503 because the database is busy or unavailable carry `Retry-After` ([ARCH-7.09](01-system-architecture.md#arch-7-database-connection-limits)).

## DEPL-6 Logging

> **DEPL-6.01** Logging uses Log4j2 (via SLF4J). Output is console-only (no file appenders by default). Log level can be set via the `loglevel` config option.

> **DEPL-6.02** Access log: `AccessLogFilter` (the first filter) logs one line per request at INFO level with the logger `org.entrystore.rowstore.LogService`, after the response is complete (for asynchronous responses such as exports, when the async request completes), in the format of 1.x (Restlet `{ciua} "{m} {rp} {rq}" {S} {ES} {es} {hh} {cig} {fi}`):
> ```
> <client address> "<method> <path> <query>" <status> <response bytes> <request bytes> <scheme>://<host> <user agent> <referrer>
> ```
> The client address is resolved like by `TrustedProxyFilter` ([SEC-5.05](07-security.md#sec-5-rate-limiting)), also for requests rejected before it (e.g. path parameters, CORS). The response bytes are the bytes of the body written by Tomcat; the request bytes are the `Content-Length`, `0` without a body, `-` for a chunked body. Missing values are empty. A request that fails with an exception is logged with status 500. Disabled with `accesslog: false` ([CFG-3.12](06-configuration.md#cfg-3-application-options)).

## DEPL-7 Shutdown

> **DEPL-7.01** Shutdown (SIGTERM) runs in two phases:
> 1. Spring Boot's graceful shutdown stops accepting requests and lets running requests finish for up to 30 seconds (`spring.lifecycle.timeout-per-shutdown-phase`, default)
> 2. `EtlProcessor.shutdown()` rejects new jobs, discards the jobs that have not started yet (temp files deleted, datasets with discarded uploads logged at WARN level, futures of exclusive tasks cancelled) and lets running jobs finish for up to 30 seconds; jobs still running then are interrupted, and their loads are rolled back
>
> Datasets of discarded and interrupted loads keep their status (ACCEPTED_DATA or PROCESSING) until the next startup sets it to ERROR ([ETL-8.06](04-etl-pipeline.md#etl-8-concurrency-control)).

> **DEPL-7.02** The shutdown can therefore take up to about 60 seconds. Container runtimes must allow at least 70 seconds before they kill RowStore (`docker stop -t 70`, Docker Compose `stop_grace_period: 70s`, Kubernetes `terminationGracePeriodSeconds: 70`); Docker's default of 10 seconds kills running requests and loads.

## Known Limitations

- No rolling restart or zero-downtime deployment support
- Shutdown interrupts ETL jobs that take longer than the 30-second grace period, and uploads that are still queued are lost (their datasets are set to ERROR at the next startup)

## References

- [System Architecture](01-system-architecture.md#arch-4-module-structure) — System overview
- [Configuration](06-configuration.md#cfg-7-cli-options) — All configuration options
- [Security](07-security.md#sec-9-reverse-proxy-contract) — Reverse proxy contract
- [README Security section](../README.md#security) — Reverse proxy requirements and nginx example
- [Glossary](12-glossary.md#glo-1-terms) — Term definitions
- Source: `RowStoreApplication.java`, `DataSourceConfig.java`, `RowStoreHealthIndicator.java`, `AccessLogFilter.java`, `EtlProcessor.java`, `bitbucket-pipelines.yml`, `Dockerfile`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: executable JAR, in-repo Dockerfile, Flyway, Actuator, graceful shutdown |
| 2026-10-20 | Added DEPL-1.05 (reverse proxy required, SEC-9); DEPL-1.04 and DEPL-5.03 notes updated |
| 2026-10-21 | DEPL-5.01: `queuedEtlProcesses` (API-3.15) |
| 2026-10-23 | DEPL-5.04: waiting for the database at startup, Retry-After (ROWS-160, ROWS-167) |
| 2026-10-23 | DEPL-1.02: arguments checked (CFG-7.05); added DEPL-2.04 (image user, `JAVA_OPTS`, heap, config location), DEPL-6.02 (access log), DEPL-7.02 (stop timeout); DEPL-7.01: queued jobs discarded at shutdown (ROWS-161, ROWS-162, ROWS-163, ROWS-166) |
