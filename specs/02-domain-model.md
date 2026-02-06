# Domain Model

## Purpose

Defines the conceptual entities, their relationships, lifecycle states, and physical database schema that underpin RowStore.

## Scope

Covers the logical data model, entity relationships, ETL status lifecycle, physical PostgreSQL schema, index strategy, and data table naming convention. For API-level interactions with these entities, see [03-api.md](03-api.md). For ETL processing details, see [04-etl-pipeline.md](04-etl-pipeline.md).

## DOM-1 Conceptual Model

> **DOM-1.01** A **Dataset** is the central entity. It has: a UUID identifier, an integer status, a creation timestamp, an ordered list of column names, and a row count.

> **DOM-1.02** An **Alias** belongs to a Dataset. It is a text string that must be alphanumeric only (letters and digits). Aliases provide human-readable alternative identifiers for datasets in API URLs.

> **DOM-1.03** A **Row** belongs to a Dataset. It has a sequential row number (`rownr`, auto-incrementing) and a JSONB `data` object containing key-value pairs where keys are column names.

> **DOM-1.04** An **ETL Job** wraps the processing of a CSV upload for a Dataset. It tracks the CSV temp file, the target dataset, and the append/replace mode. Jobs are queued and processed asynchronously.

## DOM-2 Entity Relationships

> **DOM-2.01** Dataset 1:N Alias — A dataset may have zero or more aliases. Each alias belongs to exactly one dataset.

> **DOM-2.02** Dataset 1:N Row — A dataset contains zero or more rows. Rows are stored in the dataset's dedicated data table.

> **DOM-2.03** Dataset has ETL Status — A dataset's `status` field tracks its position in the ETL lifecycle (see [DOM-3](#dom-3-etl-status-lifecycle)).

## DOM-3 ETL Status Lifecycle

> **DOM-3.01** The ETL status follows a linear state machine with one branch:
>
> ```
> CREATED (0) → ACCEPTED_DATA (1) → PROCESSING (2) → AVAILABLE (3)
>                                                   ↘ ERROR (4)
> ```
>
> - **CREATED (0)**: Dataset record exists, no data uploaded yet.
> - **ACCEPTED_DATA (1)**: CSV file received and stored as temp file, queued for processing.
> - **PROCESSING (2)**: ETL pipeline is actively parsing and inserting rows.
> - **AVAILABLE (3)**: Processing complete, dataset is queryable.
> - **ERROR (4)**: Processing failed. Dataset can be deleted or re-uploaded.

## DOM-4 Physical Database Schema

> **DOM-4.01** The **`datasets`** registry table:
> ```sql
> CREATE TABLE IF NOT EXISTS datasets (
>     id UUID PRIMARY KEY,
>     status INT NOT NULL,
>     created TIMESTAMP NOT NULL,
>     data_table CHAR(37)
> )
> ```

> **DOM-4.02** The **`aliases`** lookup table:
> ```sql
> CREATE TABLE IF NOT EXISTS aliases (
>     id SERIAL PRIMARY KEY,
>     dataset_id UUID NOT NULL,
>     alias TEXT NOT NULL
> )
> ```

> **DOM-4.03** Each dataset gets a dedicated **data table**:
> ```sql
> CREATE TABLE IF NOT EXISTS data_<uuid_no_hyphens> (
>     rownr SERIAL PRIMARY KEY,
>     data JSONB NOT NULL
> )
> ```
> The table name is `data_` followed by the dataset UUID with hyphens removed (32 hex characters), yielding a 37-character table name.

## DOM-5 Index Strategy

> **DOM-5.01** During ETL processing, a `text_pattern_ops` index is automatically created for each column:
> ```sql
> CREATE INDEX <table>_jsonidx_<hash> ON <table>
>     ((data->>'<column>') text_pattern_ops)
> ```

> **DOM-5.02** Index names use the pattern `{dataTable}_jsonidx_{md5_first_8}`, where `md5_first_8` is the first 8 characters of the MD5 hash of the column name. This avoids PostgreSQL's identifier length limits.

> **DOM-5.03** Columns where any field value exceeds 256 characters are **not indexed**. The maximum field size per column is tracked during populate and checked before index creation.

## DOM-6 Data Table Naming Convention

> **DOM-6.01** Data table names follow the pattern `data_` + UUID with hyphens removed. For example, dataset `550e8400-e29b-41d4-a716-446655440000` gets data table `data_550e8400e29b41d4a716446655440000`.

## DOM-7 JSON-LD Output

> **DOM-7.01** The dataset info endpoint returns JSON-LD-flavored output with `@context` and `@id` fields:
> ```json
> {
>     "@context": "https://entrystore.org/rowstore/",
>     "@id": "<baseURL>/dataset/<uuid>",
>     "identifier": "<uuid>",
>     "status": 3,
>     "created": "2025-01-15T10:30:00+0000",
>     "columnnames": ["name", "age", "city"],
>     "rowcount": 1500,
>     "aliases": ["mydata"]
> }
> ```
> Date format: `yyyy-MM-dd'T'HH:mm:ssZ` (ISO 8601 with timezone offset).

## Known Limitations

- No foreign key constraints between `datasets` and `aliases` tables
- No cascade delete at the database level (application handles cleanup)
- Index names use only the first 8 characters of the MD5 hash, creating a theoretical (but practically negligible) collision risk

## References

- [System Architecture](01-system-architecture.md#arch-5-component-architecture) — Overall system structure
- [ETL Pipeline](04-etl-pipeline.md#etl-5-batch-inserts) — ETL processing that populates data tables
- [Query Processing](05-querying.md#query-5-sql-construction) — How data tables are queried
- [Glossary](12-glossary.md#glo-1-terms) — Term definitions
- Source: `PgDataset.java`, `PgDatasets.java`, `EtlStatus.java`, `DatasetInfoResource.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
