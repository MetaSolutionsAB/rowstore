# System Architecture

## Purpose

Describes what RowStore is, its high-level capabilities, technology stack, module structure, component architecture, and key design decisions.

## Scope

Covers the system from a structural and architectural perspective. For detailed behavior of individual subsystems, see the linked specs: [API](03-api.md), [ETL](04-etl-pipeline.md), [Querying](05-querying.md), [Configuration](06-configuration.md).

## ARCH-1 What RowStore Is

> **ARCH-1.01** RowStore is a CSV-to-JSON pipeline with a REST query interface. It accepts CSV file uploads, processes them asynchronously, and stores the tabular data in PostgreSQL for querying via a RESTful API.

> **ARCH-1.02** Data is stored in PostgreSQL using JSONB columns, enabling schemaless storage with per-column indexing and flexible querying.

## ARCH-2 High-Level Capabilities

> **ARCH-2.01** Upload CSV datasets via HTTP POST, with automatic charset and delimiter detection.

> **ARCH-2.02** Query datasets via REST using column-name query parameters as filters, with optional regex matching.

> **ARCH-2.03** Export full datasets as JSON or CSV via streaming download.

> **ARCH-2.04** Browse datasets through a built-in web GUI with filtering, pagination, and column visibility controls.

> **ARCH-2.05** Rate limit API requests using sliding window or average rate strategies, scoped globally, per-dataset, or per-client IP.

## ARCH-3 Technology Stack

> **ARCH-3.01** Core technologies:
> - **Java 25** — Runtime and language level
> - **Spring Boot 3.5.10** — Application framework with embedded Tomcat
> - **PostgreSQL** — Data storage (JSONB)
> - **HikariCP** — JDBC connection pooling
> - **Flyway** — Database schema migration

> **ARCH-3.02** Key libraries:
> - **OpenCSV 5.11.2** — CSV parsing (RFC4180 and legacy modes)
> - **Guava 33.4.8-jre** — Rate limiting (`RateLimiter`), caching, utilities
> - **Log4j2** — Logging (via SLF4J)
> - **juniversalchardet** — Primary charset detection
> - **ICU4J** — Fallback charset detection
> - **PostgreSQL JDBC** — Database driver (managed by Spring Boot)
> - **JSON-org 20250517** — JSON processing
> - **Spring Boot Actuator** — Health checks and metrics

## ARCH-4 Module Structure

> **ARCH-4.01** RowStore is a **single Maven module** producing an executable JAR via the `spring-boot-maven-plugin`. All source code (REST controllers, ETL pipeline, store layer, configuration, filters) resides in one module.

> **ARCH-4.02** [REMOVED] `standalone/jetty` — Replaced by Spring Boot's embedded Tomcat server.

> **ARCH-4.03** [REMOVED] `standalone/common` — Replaced by Spring Boot's embedded Tomcat server.

## ARCH-5 Component Architecture

> **ARCH-5.01** The request processing chain:
> ```
> HTTP Request
>   → CorsFilter (CORS headers)
>     → ApiKeyFilter (stub — passes all requests)
>       → RateLimitFilter (optional, if configured)
>         → DispatcherServlet
>           → Controller handler method
> ```
> The `CorsFilter` is the outermost filter, providing cross-origin support. The `RateLimitFilter` is only active when rate limiting is enabled (timerange > 0 AND (global > 0 OR dataset > 0)).

> **ARCH-5.02** Read path (query): `GET /dataset/{id}?col=value` → `DatasetController` → `PgDataset.query()` → PostgreSQL prepared statement → JSON response with pagination envelope.

> **ARCH-5.03** Write path (upload): `POST /datasets` with CSV body → `DatasetsController` → temp file creation → `EtlProcessor.submit()` → 202 response → virtual thread with Semaphore permit → `PgDataset.populate()`.

## ARCH-6 Key Design Decisions

> **ARCH-6.01** **JSONB storage**: Each row is stored as a JSONB object rather than in a normalized relational schema. This enables schemaless operation (no DDL changes when column structures differ between datasets), per-column `text_pattern_ops` indexing, and flexible regex querying.

> **ARCH-6.02** **Virtual-thread ETL with Semaphore**: CSV processing happens asynchronously via virtual threads with a `Semaphore` for concurrency limiting (`maxetlprocesses`, default 5). Tasks are dispatched immediately (no polling delay), and the Semaphore blocks virtual threads cheaply until a permit is available.

> **ARCH-6.03** **Spring Boot framework**: RowStore uses Spring Boot as its application framework, providing embedded Tomcat, HikariCP connection pooling, Flyway schema migration, Actuator health/metrics endpoints, virtual thread support, and a large ecosystem of tooling and documentation.

## ARCH-7 Database Connection Limits

> **ARCH-7.01** By default, RowStore does not pool database connections: a connection is opened for a database operation and closed right afterwards, so an idle instance holds no connections (as in 1.x). Since requests run on virtual threads, the number of concurrent requests is not limited by a thread pool; instead, the number of open connections per instance is limited by **connection budgets** at the level of the data sources. This applies equally to direct JDBC connections and to `JdbcTemplate` usage, and also when the HikariCP pool is enabled (ARCH-7.07).

> **ARCH-7.02** There are three budgets, so that operations that hold a connection for a long time never use up the connections of short operations:
> - **query** — short operations of requests: queries, dataset and alias lookups and changes, status changes by requests, dataset creation and deletion, health and status checks. Limited by `maxQueryConnections` (default 10, CFG-4.05).
> - **export** — streamed exports (`GET /dataset/{id}/export`), which hold their connection while the export is transferred. Limited by `maxExportConnections` (default 2, CFG-4.06).
> - **load** — ETL jobs: loads (append and replace) and column name cleanups (ETL-11), which hold their connection for the whole job, and all other database operations of ETL jobs, in particular the status writes of a load (PROCESSING at the start, the final status, also when it is retried) and the reads of a cleanup before it obtains its connection. Limited by `maxetlprocesses` (CFG-3.03); since ETL jobs are already limited to `maxetlprocesses` (ARCH-6.02) and hold at most one load connection each at any time, they never wait for a load connection. Therefore a load finishes (and writes its final status) even if all query connections are in use for a long time.
>
> The budgets apply per database block: `database` has a query and a load budget, and an export budget if there is no `queryDatabase`; a separate `queryDatabase` (CFG-5.01) has its own query budget (for queries) and the export budget.

> **ARCH-7.03** A request for a connection of a budget whose connections are all in use waits; waiting requests get connections in FIFO order. The wait is limited by `connectionWaitTimeout` (default `querytimeout` if configured, otherwise 30 seconds, CFG-4.07). Only if no connection becomes free within that time, the request fails with **503 Service Unavailable** (like a database connectivity failure, API-6.05), unless the response status has already been sent (export). The defaults are meant to absorb normal bursts by waiting, so that clients practically never get 503.
>
> How long a request occupies a query connection is bounded, so that requests that are slow or blocked cannot use up the query connections of all other requests:
> - If `querytimeout` is configured (CFG-3.06), it applies to the dataset query and to all statements that short operations execute through the JdbcTemplates of the query budgets (e.g., the lookups of datasets and aliases, the column names that precede every query, row counts); a statement cancelled by it results in 503. Statements of short operations that change data (dataset creation and deletion, alias changes) are not limited by it.
> - All connections of the query budgets wait at most `queryLockTimeout` (default half of `connectionWaitTimeout`, CFG-4.08) for a lock (PostgreSQL `lock_timeout`, set when a connection is obtained); a lock timeout results in 503. This applies, e.g., to requests for a dataset whose data table is about to be swapped by a replace that waits for a long export or another long reader (ETL-6), or locked by a column name cleanup: such requests get 503 after the lock timeout, while requests for other datasets keep being served. In 1.x, such requests waited without limit. Half the wait timeout is used so that requests that wait for a connection behind requests blocked by a lock still get one within their wait timeout. Exports and ETL jobs keep the default lock timeout of the database (normally none). With `queryLockTimeout` 0, RowStore sets no lock timeout; then a request for a dataset blocked behind a long exclusive lock occupies its query connection until the lock is released or `querytimeout` cancels it, and requests waiting behind such requests may get 503 after their wait timeout.

> **ARCH-7.04** A request or ETL job holds at most one connection at a time: all reads that belong to an operation that holds a connection (e.g., the existing column names of an append, the availability of aliases while they are changed) use that connection, or happen before or after it is held. Otherwise requests could wait for each other until the wait timeout when a budget is exhausted.

> **ARCH-7.05** Waits for a connection longer than 1 second and wait timeouts are logged at WARN level, aggregated per budget to at most one message per minute (with the number of slow waits and timeouts since the last message, the longest wait and the current usage). The current usage of each budget (connections in use, maximum, waiting requests) is reported by `GET /status` (API-3.14) and in the details of the health indicator (`/actuator/health`, if details are shown).
>
> The `db` health indicator of Spring Boot Actuator is disabled, since it would open connections outside the budgets; the RowStore health indicator checks the primary database and, if configured separately, the query database, one after the other, with connections of their query budgets. Health and status checks wait at most 2 seconds (or the wait timeout, if shorter) for a connection, so that they answer promptly when all connections are in use: the health indicator then reports the database as `BUSY` (in use, hence reachable) and the health as UP; `GET /status` reports `datasets` as -1. The health is DOWN only if a database is not reachable.

> **ARCH-7.06** **Sizing.** The maximum number of connections of one instance is, per database:
> - without `queryDatabase`: `maxQueryConnections + maxExportConnections + maxetlprocesses` (default 10 + 2 + 5 = 17)
> - with `queryDatabase`: `database.maxQueryConnections + maxetlprocesses` on the primary database and `queryDatabase.maxQueryConnections + queryDatabase.maxExportConnections` on the query database
>
> The sum over all instances that use the same PostgreSQL cluster must stay below its `max_connections` minus the connections reserved for administration and other clients. For deployments with many instances (e.g., 80 instances x 17 = 1360 connections in the worst case), either lower the limits per instance or put a connection pooler such as **PgBouncer** in transaction pooling mode between the instances and the cluster, which multiplexes the client connections onto a small number of server connections. RowStore uses session state only within transactions (`SET LOCAL`), except for the `lock_timeout` of query connections (ARCH-7.03), which is set with `SET` when a connection is obtained, outside a transaction. With transaction pooling, such a `SET` is executed on an arbitrary server connection and stays there for other clients (including the export and load connections of other instances), while the following statements may run on another server connection without it. Therefore, with PgBouncer transaction pooling:
> - set `queryLockTimeout` to `0` in `database` and `queryDatabase` (CFG-4.08), so that RowStore changes no session state, and
> - configure the lock timeout on the server side, preferably for a separate database user of the query database, e.g., `ALTER ROLE rowstore_query SET lock_timeout = '15s'` (half of `connectionWaitTimeout`, see above). If the same user is used for loads and exports, such a setting applies to them as well: replaces set their own lock timeouts (ETL-6), but an export blocked behind an exclusive lock for longer than it fails. Relying on the values set by RowStore instead requires that PgBouncer restores `lock_timeout` per client (`track_extra_parameters`); check in the PgBouncer documentation whether this is supported for `lock_timeout` by the PgBouncer and PostgreSQL versions in use.
>
> Without any lock timeout, requests for a dataset blocked behind a long exclusive lock occupy their query connections until the lock is released or `querytimeout` cancels them, and requests waiting for these connections may get 503 after their wait timeout (see ARCH-7.03).
>
> The JDBC driver uses server-side prepared statements after 5 executions of a statement; with transaction pooling, this requires PgBouncer 1.21 or later with `max_prepared_statements` > 0, otherwise `prepareThreshold=0` has to be set for the driver.
>
> How long connections are occupied is bounded by `querytimeout` and the lock timeout (ARCH-7.03) for the query budgets; without `querytimeout`, a slow query occupies its connection until it completes.

> **ARCH-7.07** **HikariCP.** When `connectionPoolMax` > 0 (CFG-4.03), connections are pooled (and idle connections are kept, unlike the default); the budgets apply on top of the pool with the same semantics and usage reporting. The pool should be at least as large as the sum of the budgets of its database (ARCH-7.06), otherwise budgets compete for pooled connections and a connection may wait for the pool instead (with the pool's own timeout); a warning is logged at startup in that case.

## Known Limitations

- Single-process architecture (no clustering or horizontal scaling)
- No WebSocket or Server-Sent Events for ETL progress notifications

## References

- [Domain Model](02-domain-model.md#dom-4-physical-database-schema) — Data model and schema
- [REST API](03-api.md#api-3-endpoint-catalog) — REST API specification
- [ETL Pipeline](04-etl-pipeline.md#etl-1-pipeline-overview) — ETL pipeline details
- [Query Processing](05-querying.md#query-1-query-overview) — Query processing
- [Configuration](06-configuration.md#cfg-3-application-options) — Configuration options
- [Glossary](12-glossary.md#glo-1-terms) — Term definitions
- Source: `RowStoreApplication.java`, `PgRowStore.java`, `LimitedDataSource.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot 3.5.10 migration (Java 25, Tomcat, HikariCP, Flyway, virtual threads) |
| 2026-10-21 | ARCH-7: connection budgets per instance (query, export, load), one connection per request at a time, sizing |
| 2026-10-21 | ARCH-7.02: status writes of ETL jobs use the load budget; ARCH-7.03: querytimeout and lock timeout bound query connections; ARCH-7.05: health checks of both databases with a short wait |
| 2026-10-21 | ARCH-7.03, ARCH-7.06: configurable lock timeout of query connections (queryLockTimeout), configuration for PgBouncer transaction pooling |
