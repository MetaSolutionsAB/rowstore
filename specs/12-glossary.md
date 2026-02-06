# Glossary

## Purpose

Defines domain-specific terminology used throughout the RowStore specification suite.

## Scope

All terms specific to the RowStore domain. General programming or database terms are only included when RowStore assigns them a specific meaning.

## GLO-1 Terms

> **GLO-1.01 Alias** — A human-readable, alphanumeric string that can be used in place of a dataset UUID in API URLs. A dataset may have zero or more aliases. Aliases must be unique across all datasets.

> **GLO-1.02 Append Mode** — CSV upload mode (via POST to an existing dataset) that adds rows to the existing data table. Requires that the uploaded CSV has the same column structure as the existing dataset. See [ETL-6.01](04-etl-pipeline.md#etl-6-append-vs-replace).

> **GLO-1.03 Column** — A named field within a dataset, derived from the CSV header row. Column names are normalized to lowercase and trimmed during ETL processing.

> **GLO-1.04 Data Table** — The PostgreSQL table that stores a dataset's rows. Named `data_` followed by the dataset UUID with hyphens removed (32 hex characters). Each row contains a serial number and a JSONB object. See [DOM-4.03](02-domain-model.md#dom-4-physical-database-schema).

> **GLO-1.05 Dataset** — The central entity in RowStore. Represents a tabular data collection created from a CSV upload. Identified by a UUID. Has a lifecycle status, column names, rows, and optional aliases.

> **GLO-1.06 ETL** — Extract, Transform, Load. The asynchronous pipeline that processes uploaded CSV files into queryable JSONB rows in PostgreSQL. See [04-etl-pipeline.md](04-etl-pipeline.md).

> **GLO-1.07 ETL Status** — An integer indicating the processing state of a dataset: CREATED (0), ACCEPTED_DATA (1), PROCESSING (2), AVAILABLE (3), or ERROR (4). See [DOM-3.01](02-domain-model.md#dom-3-etl-status-lifecycle).

> **GLO-1.08 Export** — Bulk download of an entire dataset as JSON or CSV. Uses streaming with configurable page sizes. See [API-3.10](03-api.md#api-3-endpoint-catalog).

> **GLO-1.09 JSONB** — PostgreSQL's binary JSON column type. Used to store each dataset row as a schemaless key-value object, enabling per-column indexing and flexible querying.

> **GLO-1.10 JSONP** — JSON with Padding. A technique for cross-origin data access that wraps JSON responses in a JavaScript function call. Activated via the `_callback` query parameter. See [API-7.01](03-api.md#api-7-jsonp-support).

> **GLO-1.11 Pagination** — The mechanism for retrieving query results in pages using `_limit` and `_offset` parameters. The response envelope includes `prev` and `next` URLs for navigation. See [QUERY-6](05-querying.md#query-6-pagination).

> **GLO-1.12 Populate** — The internal operation that inserts parsed CSV data into a dataset's data table. Includes batch inserts (every 200 rows), column indexing, and transaction management. See [ETL-5](04-etl-pipeline.md#etl-5-batch-inserts).

> **GLO-1.13 Purge** — Deletion of a dataset, including its data table, registry entry, and all aliases. Only permitted when the dataset is in AVAILABLE or ERROR status. See [API-3.07](03-api.md#api-3-endpoint-catalog).

> **GLO-1.14 Query** — A GET request to a dataset endpoint with column-name query parameters as filters. Supports exact match and regex modes. See [05-querying.md](05-querying.md).

> **GLO-1.15 Rate Limit (Average)** — A rate limiting strategy using Guava's `RateLimiter`. Enforces a steady request rate (permits per second = limit / timerange). Non-blocking; does not provide a `Retry-After` header. See [SEC-5.02](07-security.md#sec-5-rate-limiting).

> **GLO-1.16 Rate Limit (Sliding Window)** — A rate limiting strategy using Guava Cache with TTL-based expiration. Tracks individual request timestamps and provides a `Retry-After` header when the limit is exceeded. See [SEC-5.01](07-security.md#sec-5-rate-limiting).

> **GLO-1.17 Read Replica** — An optional secondary PostgreSQL database configured for query routing. When configured via `queryDatabase`, all read queries are directed to this connection while writes use the primary. See [CFG-5](06-configuration.md#cfg-5-read-replica-configuration).

> **GLO-1.18 Regex Mode** — The query matching strategy: `disabled` (exact match only), `simple` (regex only with `^` prefix), or `full` (heuristic detection plus `~` prefix override). See [QUERY-4](05-querying.md#query-4-regex-modes).

> **GLO-1.19 Replace Mode** — CSV upload mode (via PUT) that truncates the existing data table and reloads from the new CSV. See [ETL-6.02](04-etl-pipeline.md#etl-6-append-vs-replace).

> **GLO-1.20 Row** — A single record within a dataset, stored as a JSONB object in the data table. Each row has a sequential `rownr` (serial primary key) and a `data` column containing the key-value pairs.

> **GLO-1.21 RowStore** — A CSV-to-JSON pipeline with a REST query interface. Accepts CSV uploads, processes them asynchronously, stores the data in PostgreSQL JSONB columns, and exposes it via a RESTful API.

> **GLO-1.22 Swagger/OpenAPI** — A per-dataset, dynamically generated OpenAPI specification that describes the dataset's query parameters based on its column names and the server configuration. Available at `/dataset/{id}/swagger`.

## Known Limitations

- Terms are limited to the current RowStore feature set. As new features are added, corresponding terms should be added here.

## References

- All spec files in this suite reference these definitions
- [specs/README.md](README.md) — Specification index

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
