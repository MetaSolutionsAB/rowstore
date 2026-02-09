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
- Source: `RowStoreApplication.java`, `PgRowStore.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot 3.5.10 migration (Java 25, Tomcat, HikariCP, Flyway, virtual threads) |
