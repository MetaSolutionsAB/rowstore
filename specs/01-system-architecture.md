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
> - **Java 21** — Runtime and language level
> - **Restlet 2.5.1** — REST framework
> - **PostgreSQL** — Data storage (JSONB)
> - **Jetty 9.4.57** — Embedded HTTP server (standalone mode)

> **ARCH-3.02** Key libraries:
> - **OpenCSV 5.11.2** — CSV parsing (RFC4180 and legacy modes)
> - **Guava 33.4.8-jre** — Rate limiting (`RateLimiter`), caching, utilities
> - **Log4j2 2.25.0** — Logging (via SLF4J 2.0.17)
> - **juniversalchardet** — Primary charset detection
> - **ICU4J** — Fallback charset detection
> - **PostgreSQL JDBC 42.7.7** — Database driver
> - **JSON-org 20250517** — JSON processing

## ARCH-4 Module Structure

> **ARCH-4.01** **`webapp`** — Core application module. Contains the Restlet-based REST API, ETL pipeline, PostgreSQL storage layer, web GUI templates, and all resource handlers. Produces a WAR artifact.

> **ARCH-4.02** **`standalone/jetty`** — Embedded Jetty server for standalone deployment. Wraps the webapp module with CLI argument parsing, HTTP/2 support, and connector configuration. Produces a distributable tarball.

> **ARCH-4.03** **`standalone/common`** — Shared standalone utilities used by the Jetty module for configuration resolution and application bootstrap.

## ARCH-5 Component Architecture

> **ARCH-5.01** The request processing chain:
> ```
> HTTP Request
>   → JSCallbackFilter (JSONP wrapping)
>     → RateLimitFilter (optional, if configured)
>       → Router
>         → Resource handler
> ```
> The JSCallbackFilter is the outermost filter. The RateLimitFilter is only attached when rate limiting is enabled (timerange > 0 AND (global > 0 OR dataset > 0)).

> **ARCH-5.02** Read path (query): `GET /dataset/{id}?col=value` → `DatasetResource.representJson()` → `PgDataset.query()` → PostgreSQL prepared statement → JSON response with pagination envelope.

> **ARCH-5.03** Write path (upload): `POST /datasets` with CSV body → `DatasetsResource.acceptCSV()` → temp file creation → `EtlProcessor.submit()` → 202 response → async `DatasetSubmitter` poll → `DatasetLoader` thread → `PgDataset.populate()`.

## ARCH-6 Key Design Decisions

> **ARCH-6.01** **JSONB storage**: Each row is stored as a JSONB object rather than in a normalized relational schema. This enables schemaless operation (no DDL changes when column structures differ between datasets), per-column `text_pattern_ops` indexing, and flexible regex querying.

> **ARCH-6.02** **Queue-based async ETL**: CSV processing happens asynchronously via a `ConcurrentLinkedQueue` with configurable concurrency (`maxetlprocesses`, default 5). This decouples upload latency from processing time and prevents resource exhaustion from concurrent large uploads.

> **ARCH-6.03** **Restlet framework**: RowStore uses Restlet as its REST framework, providing resource-oriented routing, content negotiation, and filter chains. Each endpoint is a `ServerResource` subclass with annotated HTTP method handlers.

## Known Limitations

- Single-process architecture (no clustering or horizontal scaling)
- No built-in metrics or APM integration
- No WebSocket or Server-Sent Events for ETL progress notifications
- Jetty 9.x (Jakarta EE 8, not Jakarta EE 9+)

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
