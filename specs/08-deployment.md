# Deployment & Operations

## Purpose

Documents how to build, deploy, and operate a RowStore instance, including standalone Jetty deployment, Docker, CI/CD, PostgreSQL requirements, monitoring, logging, and shutdown behavior.

## Scope

Covers operational concerns. For configuration options, see [06-configuration.md](06-configuration.md). For the system architecture being deployed, see [01-system-architecture.md](01-system-architecture.md).

## DEPL-1 Standalone Jetty

> **DEPL-1.01** Build the distributable:
> ```bash
> mvn -Dmaven.test.skip=true install
> ```
> This produces a tarball at `standalone/jetty/target/dist/`.

> **DEPL-1.02** Run the server:
> ```bash
> chmod +x standalone/jetty/target/dist/bin/rowstore
> standalone/jetty/target/dist/bin/rowstore <config-file> [port]
> ```
> The config file is required (or set `ROWSTORE_CONFIG_URI`). Port defaults to 8282.

> **DEPL-1.03** CLI options for tuning:
> - `-c, --config <URI>` — Configuration file
> - `-p, --port <PORT>` — Listen port (default 8282)
> - `-l, --log-level <LEVEL>` — Log level
> - `--connector-params <SETTINGS>` — Jetty connector parameters (comma-separated key=value)
>
> See [CFG-7](06-configuration.md#cfg-7-cli-options) for details.

> **DEPL-1.04** The standalone Jetty deployment supports HTTP/2 and respects `useForwardedForHeader=true` for running behind reverse proxies. Access log format: `{ciua} "{m} {rp} {rq}" {S} {ES} {es} {hh} {cig} {fi}`.

## DEPL-2 Docker

> **DEPL-2.01** Official Docker image: `metasolutions/rowstore:<version>` on Docker Hub.

> **DEPL-2.02** The Dockerfile is maintained in an external repository (`bitbucket.org:metasolutions/docker.git`), separate from the main RowStore source.

> **DEPL-2.03** Docker tags follow these conventions:
> - Version-specific: `1.8`, `1.8-SNAPSHOT`
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
> - Standalone tarball (`.tar.gz`)
> - SHA256 checksum
> - GPG signature
> - Uploaded via SCP to metasolutions.se

## DEPL-4 PostgreSQL Requirements

> **DEPL-4.01** PostgreSQL 9.4+ is required for JSONB column support. Recommended: PostgreSQL 16 (used in CI/CD tests).

> **DEPL-4.02** RowStore auto-creates its schema on startup using `CREATE TABLE IF NOT EXISTS` for the `datasets` and `aliases` tables. No manual schema migration is needed.

## DEPL-5 Monitoring

> **DEPL-5.01** `GET /status` returns:
> ```json
> {
>     "service": "RowStore",
>     "version": "1.8-SNAPSHOT",
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

> **DEPL-5.03** There is no built-in metrics export (no Prometheus endpoint, no StatsD, no APM agent). Monitoring relies on the `/status` endpoint and external log analysis.

## DEPL-6 Logging

> **DEPL-6.01** Logging uses Log4j2 (via SLF4J). Output is console-only (no file appenders by default). Log level can be set via the `loglevel` config option or the `-l` CLI argument. Dynamic log level changes take effect at startup only.

## DEPL-7 Shutdown

> **DEPL-7.01** Graceful shutdown is handled by `RowStore.shutdown()`:
> - Interrupts the ETL `DatasetSubmitter` thread
> - Deregisters JDBC drivers to prevent classloader leaks
> - Active ETL processing may be interrupted mid-operation

## Known Limitations

- No health check endpoint beyond `/status` (no readiness/liveness probes)
- No metrics export for monitoring systems
- No rolling restart or zero-downtime deployment support
- No database migration tooling (schema changes require manual intervention)
- Shutdown may interrupt active ETL jobs without cleanup

## References

- [System Architecture](01-system-architecture.md#arch-4-module-structure) — System overview
- [Configuration](06-configuration.md#cfg-7-cli-options) — All configuration options
- [Security](07-security.md#sec-5-rate-limiting) — Security considerations for deployment
- [Glossary](12-glossary.md#glo-1-terms) — Term definitions
- Source: `RowStoreApplicationStandaloneJetty.java`, `RowStoreApplicationStandalone.java`, `bitbucket-pipelines.yml`, `log4j2.properties`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
