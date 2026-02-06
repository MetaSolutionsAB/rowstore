# ETL Pipeline

## Purpose

Documents the Extract-Transform-Load pipeline that processes CSV uploads into queryable JSONB data in PostgreSQL.

## Scope

Covers the full upload-to-available lifecycle: file reception, queue mechanics, charset/separator detection, CSV parsing, batch inserts, indexing, and concurrency control. For the API endpoints that trigger ETL, see [03-api.md](03-api.md). For the resulting data model, see [02-domain-model.md](02-domain-model.md).

## ETL-1 Pipeline Overview

> **ETL-1.01** The ETL pipeline follows this sequence:
> ```
> CSV Upload → Temp File → Queue (ConcurrentLinkedQueue)
>   → DatasetSubmitter (poll thread)
>     → DatasetLoader (worker thread)
>       → Charset Detection → Separator Detection → CSV Parsing
>         → Batch Insert (every 200 rows) → Index Creation
>           → Status: AVAILABLE
> ```
> The upload returns 202 immediately. Processing is entirely asynchronous.

## ETL-2 Queue Mechanics

> **ETL-2.01** The ETL queue is a `ConcurrentLinkedQueue<EtlResource>`. The `DatasetSubmitter` thread polls it in a loop with a 5-second sleep interval between checks.

> **ETL-2.02** The maximum number of concurrent processing threads is controlled by the `maxetlprocesses` configuration option (default: 5). A new `DatasetLoader` thread is started only when `runningConversions < concurrentConversions`.

## ETL-3 Status Lifecycle

> **ETL-3.01** The dataset status transitions through:
>
> | From | To | Trigger |
> |------|----|---------|
> | — | CREATED (0) | Dataset record created in registry |
> | CREATED (0) | ACCEPTED_DATA (1) | CSV file received and temp file written |
> | ACCEPTED_DATA (1) | PROCESSING (2) | DatasetLoader thread starts processing |
> | PROCESSING (2) | AVAILABLE (3) | All rows inserted and indexes created |
> | PROCESSING (2) | ERROR (4) | Exception during parsing or insertion |

> **ETL-3.02** On error, the dataset status is set to ERROR (4). The temp file is deleted. The dataset can be deleted via the API or re-uploaded (PUT replaces, POST appends).

## ETL-4 CSV Processing

> **ETL-4.01** Charset detection uses a two-stage fallback:
> 1. **Primary**: juniversalchardet `UniversalDetector` — reads up to 512 KB of the file
> 2. **Fallback**: ICU4J `CharsetDetector`
> 3. **Default**: UTF-8 if neither detector returns a result

> **ETL-4.02** Separator detection examines the first two lines of the CSV:
> - Count semicolons (`;`) in lines 1 and 2
> - If both counts are > 0 and equal, use `;` as separator
> - Otherwise, default to `,` (comma)

> **ETL-4.03** Parser selection is controlled by the `legacyparser` configuration option:
> - **Default** (`legacyparser: false`): `RFC4180ParserBuilder` with `quoteChar='"'`
> - **Legacy** (`legacyparser: true`): `CSVParserBuilder` with `quoteChar='"'`
>
> Both use OpenCSV's `CSVReaderBuilder` with the selected parser.

> **ETL-4.04** Column name normalization:
> - All column names are converted to **lowercase**
> - Column names are **trimmed** of whitespace
> - Empty column labels are **skipped** (not included in the dataset)

## ETL-5 Batch Inserts

> **ETL-5.01** Rows are inserted in batches of **200**. Auto-commit is disabled during populate. A `stmt.executeBatch()` call is issued every 200 rows and at the end of the file. On any error, the entire transaction is rolled back.

## ETL-6 Append vs Replace

> **ETL-6.01** **POST** to an existing dataset appends rows. The uploaded CSV's column structure must match the existing dataset's columns (validated before processing).

> **ETL-6.02** **PUT** to an existing dataset truncates the data table (deletes all existing rows) and reloads from the new CSV. Column structure may differ from the previous upload.

## ETL-7 Auto-Indexing

> **ETL-7.01** After all rows are inserted, a `text_pattern_ops` index is created for each column on the JSONB-extracted text value:
> ```sql
> CREATE INDEX <table>_jsonidx_<md5> ON <table>
>     ((data->>'<column>') text_pattern_ops)
> ```
> Column names are escaped via `BaseConnection.escapeString()` to prevent SQL injection in DDL.

> **ETL-7.02** Columns where any field value exceeds **256 characters** are not indexed. Field sizes are tracked per column during insertion using a `columnSize` HashMap.

## ETL-8 Concurrency Control

> **ETL-8.01** A `runningConversions` counter tracks active `DatasetLoader` threads. The `DatasetSubmitter` only dequeues a new job when this count is below `concurrentConversions`. If a dataset is already in PROCESSING state when a new upload arrives for the same dataset, the system busy-waits until the current processing completes.

## ETL-9 Temp File Lifecycle

> **ETL-9.01** Uploaded CSV content is written to a temp file named `RowStore-*.csv` via `File.createTempFile()`. The file is marked with `deleteOnExit()` as a safety net. It is explicitly deleted after successful processing or on early failure (before queuing).

## Known Limitations

- No progress reporting during ETL processing (no percentage or row count updates)
- Separator detection only checks semicolon vs comma (no support for tab or pipe auto-detection)
- Batch size (200) is hardcoded, not configurable
- No retry mechanism for failed ETL jobs
- Column structure validation for append mode happens during processing, not at upload time

## References

- [Domain Model](02-domain-model.md#dom-4-physical-database-schema) — Entity model and database schema
- [REST API](03-api.md#api-3-endpoint-catalog) — Upload endpoints (POST /datasets, POST/PUT /dataset/{id})
- [Configuration](06-configuration.md#cfg-3-application-options) — `maxetlprocesses`, `legacyparser` options
- [Glossary](12-glossary.md#glo-1-terms) — ETL, Populate, Append Mode, Replace Mode
- Source: `EtlProcessor.java`, `PgDataset.java` (populate method), `DatasetUtil.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
