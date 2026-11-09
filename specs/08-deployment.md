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

> **DEPL-1.06** A 1.x WAR deployed under a context path is replaced by `--server.servlet.context-path=<path>` (e.g. `/rowstore`); response URLs are built from `baseurl` ([CFG-3.01](06-configuration.md#cfg-3-application-options)) as before. Templates (`webgui_*.html`, `swagger.json_template`) are read from the JAR and cannot be overridden by files on the classpath as with the `etc/` directory of the 1.x distribution.

## DEPL-2 Docker

> **DEPL-2.01** Official Docker image: `metasolutions/rowstore:<version>` on Docker Hub.

> **DEPL-2.02** The Dockerfile is maintained **in the RowStore repository** (root `Dockerfile`). It uses `eclipse-temurin:25-jre-alpine` as the base image and copies the pre-built JAR.

> **DEPL-2.04** The image runs RowStore as the unprivileged user `rowstore` (UID and GID 10001), with the working directory `/srv/rowstore` (owned by `rowstore`; the jar is owned by root with mode 0644, set with `COPY --chmod` independently of the build machine's umask, so neither `rowstore` nor group 0, as which OpenShift runs containers, can write it; `COPY --chmod` requires BuildKit). A mounted configuration must be readable for UID or GID 10001; if it is readable only by root or the host user, it is made readable for the group (`chgrp 10001 rowstore.json && chmod 0640 rowstore.json`), or the container is run as a user that can read it (`docker run --user <uid>`). The configuration location is passed as the environment variable `ROWSTORE_CONFIG_URI` (set to `file:///srv/rowstore/rowstore.json` with `ENV`; a container overrides it to read another location), so arguments of `docker run` are passed to RowStore without replacing it; `--rowstore.config.uri` takes precedence over the variable (CFG-2.01). The entrypoint runs the base image's `/__cacert_entrypoint.sh` (DEPL-2.05), which then runs `sh -c 'set -f; exec java -XX:MaxRAMPercentage=75 $JAVA_OPTS -jar rowstore.jar "$@"'`: JVM options are passed in `JAVA_OPTS` (as with the 1.x start script), split into words without glob expansion, and take precedence over the default heap of 75% of the container's memory limit, and both shells are replaced by java (`exec`), so it receives the stop signal.

> **DEPL-2.03** Docker tags follow these conventions (pushed by the Docker Hub step of the pipeline, DEPL-3.02):
> - Version-specific: the version in `VERSION.txt`, e.g. `2.0.0`, `2.0-SNAPSHOT` (master and develop)
> - Major.minor shorthand, e.g. `2.0` (master)
> - `latest` — Latest release (master), see DEPL-2.05
> - `develop` — Latest development build (develop)

> **DEPL-2.05** The image is a drop-in replacement for the 1.x images, which were built in the separate `docker` repository (directory `rowstore/`), ran `bin/rowstore --port 8080` as root and exposed 8080:
> - **Port:** the image listens on **8080** like the 1.x images (`ENV SERVER_PORT=8080`, `EXPOSE 8080`); `--server.port=<port>` passed to `docker run` takes precedence over the environment variable. The JAR's own default port stays 8282 (DEPL-1.02).
> - **Tags:** the 2.0 images are pushed to the same repository `metasolutions/rowstore` as the 1.x images. The Docker Hub step also tags and pushes `latest` on master, so `latest` moves from 1.7 to 2.0 with the 2.0 release; deployments that stay on 1.x pull `1.7` (or `1.7.0`). The `docker` repository builds 1.x images only: its `build.sh` refuses versions 2.0 and later, also without an argument once `latest.version` is 2.0 or later.
> - **Configuration:** 1.x required `-c <URI>` unless `ROWSTORE_CONFIG_URI` was set, and the 1.x images set neither, so every working 1.x container definition sets one of them. A definition that sets `ROWSTORE_CONFIG_URI` works unchanged, e.g. `file:///srv/rowstore/etc/rowstore.json` with the configuration mounted there; without it, the 2.0 image reads `/srv/rowstore/rowstore.json` (DEPL-2.04). `-c <URI>` is rejected (CFG-7.05) and must become `-e ROWSTORE_CONFIG_URI=<URI>` or `--rowstore.config.uri=<URI>`.
> - **CA certificates:** the entrypoint of the 1.x images was the base image's `/__cacert_entrypoint.sh` (Eclipse Temurin), which imports `/certificates/*.crt` into the truststore of the JVM if `USE_SYSTEM_CA_CERTS` is set (e.g. for `"ssl": true` with a private CA). The 2.0 image runs it in front of RowStore. As the user `rowstore` cannot write the JVM's truststore, the script copies it to a temporary file in `/tmp` (which must be writable), adds the certificates there and points the JVM to it with `JAVA_TOOL_OPTIONS`; the system's CA store is not updated (it was for root in 1.x), which affects only tools other than java.
> - An existing 1.x container definition works with the 2.0 image after changing only the tag, as long as the configuration is readable for UID or GID 10001 (DEPL-2.04) and contains no options that 2.0 rejects (CFG-7.05).

## DEPL-3 CI/CD

> **DEPL-3.01** CI/CD uses Bitbucket Pipelines with this workflow:
> ```
> Build (mvn install, skip ITs)
>   → Integration Test (Testcontainers PostgreSQL 16) + Integration Test PostgreSQL 9.6 (subset, in parallel)
>     → Deploy (artifacts + signatures) + Docker Hub push
> ```

> **DEPL-3.02** Branch strategy:
> - **master**: Full pipeline — build, test, deploy, Docker push (`<version>`, `<major.minor>` and `latest` tags)
> - **develop**: Full pipeline — build, test, deploy, Docker push (`<version>` and `develop` tags)
> - **Other branches**: Build and test only (no deploy, no Docker push)

> **DEPL-3.03** Deploy artifacts:
> - Executable JAR (`.jar`)
> - SHA256 checksum
> - GPG signature
> - Uploaded via SCP to metasolutions.se

## DEPL-4 PostgreSQL Requirements

> **DEPL-4.01** PostgreSQL 9.6 or later is required (JSONB since 9.4; replaces and the cleanup of column names use `idle_in_transaction_session_timeout` and `to_regclass(text)`, available since 9.6). At startup, RowStore checks `server_version_num` of the primary database and does not start with an older version (the error names the required and the found version); like the other startup maintenance, the check waits for an unreachable database (ARCH-7.08), and if the version cannot be determined for another reason, an error is logged and the startup continues. Recommended: PostgreSQL 16 (used in CI/CD tests). The integration tests can be run against another version with `-Drowstore.test.postgresImage=postgres:9.6-alpine`; for 2.0, the integration tests of CSV parsing, replaces, the cleanup of column names, queries and exports were run against PostgreSQL 9.6 (ROWS-165). CI runs a subset of the integration tests against `postgres:9.6-alpine` in parallel with the full suite on PostgreSQL 16 (DEPL-3.01): `ColumnNameCleanupIT`, `DatasetReplaceIT`, `DatasetDeleteIT`, `CsvBomIT`, `DatasetUtf8LifecycleIT`, `DatasetEmptyColumnIT`, which cover the SQL of loads, replaces, deletions, the migrations and the cleanup of column names.

> **DEPL-4.02** Schema is managed by **Flyway** with `baselineOnMigrate=true`. On first startup against an existing database, Flyway adopts the current schema. New migrations are applied automatically. The initial migration (`V1__initial_schema.sql`) creates the `datasets` and `aliases` tables.

> **DEPL-4.03** The database user needs the `CREATE` privilege on its schema: Flyway creates `flyway_schema_history` and the migrations create `maintenance_tasks` at the first start, datasets get their own data tables, and every replace creates a shadow table (ETL-6.02). Without it, the migration fails and RowStore does not start.

> **DEPL-4.04** Rollback to 1.7: 1.7 runs on a database that 2.0 has used and ignores `flyway_schema_history` and `maintenance_tasks`. Column names renamed by the cleanup (ETL-11) stay renamed, and 1.7 stores byte order marks and soft hyphens in the column names of new uploads again. The data tables of all datasets created or replaced by 2.0 (DOM-4.03) keep `rownr BIGSERIAL`; 1.7 works with them, since it uses `rownr` only for sorting and counting and empties the table with `TRUNCATE` on a replace (data tables created by 1.7 get `rownr SERIAL` again). Before upgrading to 2.0 again, the row `column-name-cleanup` must be deleted from `maintenance_tasks` (ETL-11.07), so that the cleanup checks the datasets loaded by 1.7. Leftover shadow tables are dropped at the next start of 2.0, not by 1.7. The same applies to data tables of datasets deleted by 2.0 whose background drop has not happened yet (ETL-12.03); 1.7 ignores them. The aliases of deleted datasets that the migration `V3__delete_orphan_aliases.sql` removed (DOM-2.04) were unusable in 1.7 as well, so 1.7 is not affected by their removal. 1.7 leaves the aliases of datasets it deletes behind again, and since the migration has already been applied, upgrading to 2.0 again does not remove them; they can be removed with the statement of the migration (`DELETE FROM aliases a WHERE NOT EXISTS (SELECT 1 FROM datasets d WHERE d.id = a.dataset_id)`).

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

> **DEPL-5.04** While the database is not reachable at startup, RowStore waits for it without a time limit and does not listen on its port ([ARCH-7.08](01-system-architecture.md#arch-7-database-connection-limits)): the HTTP port is opened only after the database is reachable (unlike 1.7, which answered `/status` with `datasets` -1 meanwhile), so health and liveness checks fail to connect until then. An orchestrator should not restart the instance only because it does not answer yet during that time: give health checks a generous start period (e.g., a Kubernetes `startupProbe` with a high `failureThreshold`, or `start_period` of a Docker health check). Only connectivity failures are waited for; authentication and configuration errors (e.g., wrong password, missing database) fail the startup immediately, also with a connection pool. Responses with 503 because the database is busy or unavailable carry `Retry-After` ([ARCH-7.09](01-system-architecture.md#arch-7-database-connection-limits)).

## DEPL-6 Logging

> **DEPL-6.01** Logging uses Log4j2 (via SLF4J). Output is console-only (no file appenders by default). Log level can be set via the `loglevel` config option.

> **DEPL-6.02** Access log: `AccessLogFilter` (the first filter) logs one line per request at INFO level with the logger `org.entrystore.rowstore.LogService`, after the response is complete (for asynchronous responses such as exports, when the async request completes), in the format of 1.x (Restlet `{ciua} "{m} {rp} {rq}" {S} {ES} {es} {hh} {cig} {fi}`):
> ```
> <client address> "<method> <path> <query>" <status> <response bytes> <request bytes> <scheme>://<host> <user agent> <referrer>
> ```
> The client address is resolved like by `TrustedProxyFilter` ([SEC-5.05](07-security.md#sec-5-rate-limiting)), also for requests rejected before it (e.g. path parameters, CORS). The response bytes are the bytes of the body written by Tomcat, also for streamed responses such as exports (unlike 1.x, which logged the declared size of the response entity and left the field empty if it was unknown); the request bytes are the `Content-Length`, `0` without a body, and empty for a chunked body of unknown size, as in 1.x. Missing values are empty. As in 1.x, every request is logged, including requests to `/actuator/**` such as health probes (`/actuator/health`, `/actuator/health/liveness`, `/actuator/health/readiness`), so frequent probes add lines to the log. A request that fails with an exception is logged with status 500. Disabled with `accesslog: false` ([CFG-3.12](06-configuration.md#cfg-3-application-options)).

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
| 2026-10-23 | DEPL-1.02: arguments checked (CFG-7.05); added DEPL-2.04 (image user, readable configuration, `JAVA_OPTS`, heap, config location), DEPL-6.02 (access log of every request), DEPL-7.02 (stop timeout); DEPL-7.01: queued jobs discarded at shutdown (ROWS-161, ROWS-162, ROWS-163, ROWS-166) |
| 2026-10-23 | DEPL-4.01: PostgreSQL 9.6+ (was 9.4+), checked at startup (ROWS-165) |
| 2026-10-23 | Added DEPL-1.06 (context path, templates), DEPL-4.03 (CREATE privilege), DEPL-4.04 (rollback to 1.7) (ROWS-168) |
| 2026-11-05 | DEPL-4.04: all data tables created or replaced by 2.0 have `rownr BIGSERIAL` (ROWS-168) |
| 2026-11-05 | DEPL-3.01, DEPL-4.01: CI runs a subset of the integration tests against PostgreSQL 9.6 (ROWS-165) |
| 2026-11-05 | DEPL-5.04: port opened only after the database is reachable, unlimited wait, start period of health checks, fast failure of authentication and configuration errors |
| 2026-11-09 | DEPL-4.04: data tables of deleted datasets and removed aliases after a rollback to 1.7 (ROWS-174) |
| 2026-11-09 | Added DEPL-2.05 (image listens on 8080 like the 1.x images, `latest` pushed on master, `docker` repository builds 1.x only); DEPL-2.03 and DEPL-3.02: tags per branch, `latest` (ROWS-176) |
| 2026-11-09 | DEPL-2.04, DEPL-2.05: configuration falls back to `/srv/rowstore/etc/rowstore.json`, `ROWSTORE_CONFIG_URI` no longer set with `ENV`; base image entrypoint for `USE_SYSTEM_CA_CERTS` kept (ROWS-176) |
| 2026-11-09 | DEPL-2.04, DEPL-2.05: fallback to `/srv/rowstore/etc/rowstore.json` dropped (1.x required `ROWSTORE_CONFIG_URI` or `-c`), `ROWSTORE_CONFIG_URI` set with `ENV` again; `JAVA_OPTS` not glob-expanded (ROWS-176) |
| 2026-11-09 | DEPL-2.04: jar copied with mode 0644 (`COPY --chmod`, BuildKit), not writable for group 0 (ROWS-176) |
