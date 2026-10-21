# ETL Pipeline

## Purpose

Documents the Extract-Transform-Load pipeline that processes CSV uploads into queryable JSONB data in PostgreSQL.

## Scope

Covers the full upload-to-available lifecycle: file reception, virtual thread dispatch, charset/separator detection, CSV parsing, batch inserts, indexing, and concurrency control. For the API endpoints that trigger ETL, see [03-api.md](03-api.md). For the resulting data model, see [02-domain-model.md](02-domain-model.md).

## ETL-1 Pipeline Overview

> **ETL-1.01** The ETL pipeline follows this sequence:
> ```
> CSV Upload → Temp File → EtlProcessor.submit()
>   → Virtual Thread (Semaphore-gated)
>     → Charset Detection → Separator Detection → CSV Parsing
>       → Batch Insert (every 200 rows) → Index Creation
>         → (replace only) Shadow Table replaces Data Table
>           → Status: AVAILABLE
> ```
> The upload returns 202 immediately. Processing is entirely asynchronous.

## ETL-2 Concurrency Mechanism

> **ETL-2.01** The ETL processor uses an `ExecutorService` with virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`) and a `Semaphore` for concurrency limiting. Tasks are dispatched immediately — there is no polling delay.

> **ETL-2.02** The maximum number of concurrent processing tasks is controlled by the `maxetlprocesses` configuration option (default: 5). The `Semaphore` is initialized with this value, and each task acquires a permit before processing.

## ETL-3 Status Lifecycle

> **ETL-3.01** The dataset status transitions through:
>
> | From | To | Trigger |
> |------|----|---------|
> | — | CREATED (0) | Dataset record created in registry |
> | CREATED (0) | ACCEPTED_DATA (1) | CSV file received and temp file written |
> | ACCEPTED_DATA (1) | PROCESSING (2) | Virtual thread begins processing |
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
> - Leading and trailing characters up to U+0020 (spaces and control characters, as by `String.trim()` in 1.x) and Unicode whitespace (as by `String.strip()`, e.g., U+2003) are removed; no-break spaces (e.g., U+00A0) are kept, as in 1.x
> - Byte order marks and soft hyphens are removed (ETL-4.07)
> - Empty column labels are **skipped** (not included in the dataset)

> **ETL-4.05** Empty lines: a line without any characters outside a quoted value is an empty line, not a record. Empty lines at the end of the file are ignored. An empty line that is followed by further records, including an empty line directly after the header, fails the load: the line number of the first empty line is logged at ERROR level, the transaction is rolled back, and the status is set to ERROR; a failed replace keeps the previous data (ETL-6.03). Lines consisting of whitespace only are records, and empty lines inside a quoted multi-line value are part of the value. Both parsers (ETL-4.03) behave the same; since they do not report empty lines reliably (`RFC4180Parser` returns `null`, which must not be taken for the end of the file, and `CSVParser` returns the same record `[""]` as for a quoted empty value `""`), an empty line is detected as a record that consists of exactly one physical line which is empty. As intended consequence, a single-column CSV cannot express an empty value as an empty line between rows; the value must be quoted (`""`). In 1.x, the default parser silently dropped all rows after an empty line and the legacy parser loaded an empty line as a row.

> **ETL-4.06** Byte order mark: a byte order mark (U+FEFF) at the beginning of the file is skipped before the text reaches the CSV parser, and the separator detection (ETL-4.02) reads the same text without it. Java's decoders keep the byte order mark for the charsets that charset detection (ETL-4.01) returns for such files (UTF-8, UTF-16LE, UTF-16BE), so it is skipped after decoding: exactly one U+FEFF as first character. Without this, the label of the first column started with the byte order mark, and since the default parser only treats a value as quoted if it starts with a quote, a quoted first label kept its quotes: the header `<BOM>"kopare_id",...` was stored as the column name `<BOM>"kopare_id"` (the legacy parser stored `<BOM>kopare_id`). Now both parsers yield `kopare_id`. A first line consisting of a byte order mark only is an empty line (ETL-4.05). Malformed input is still reported as an error of the load.

> **ETL-4.07** Label sanitizing: as a second line of defence, byte order marks (U+FEFF) and soft hyphens (U+00AD) are removed anywhere in a label before it is stripped and converted to lowercase (ETL-4.04). Values are not changed. The comparison of an append with the stored column names (ETL-6.01) uses the sanitized labels. As a consequence, column names that contained these characters in 1.x differ from those of the same CSV file loaded by 2.0; existing datasets are renamed accordingly (ETL-11), which changes their column names as visible in queries, `/info`, `/swagger` and exports.

## ETL-5 Batch Inserts

> **ETL-5.01** Rows are inserted in batches of **200**. Auto-commit is disabled during populate. A `stmt.executeBatch()` call is issued every 200 rows and at the end of the file. On any error, the entire transaction is rolled back.

## ETL-6 Append vs Replace

> **ETL-6.01** **POST** to an existing dataset appends rows. The uploaded CSV's column structure must match the existing dataset's columns (validated before processing). If the stored column names differ from the labels only because they were stored before 2.0 without cleaning, they are renamed with the append (ETL-11.04).

> **ETL-6.02** **PUT** to an existing dataset replaces all existing rows with the rows of the new CSV. Column structure may differ from the previous upload. The data table is not truncated: within one transaction, the new rows are loaded into a shadow table `<dataTable>_new` (created with the same DDL as data tables, see DOM-4.03 and DOM-6.02, after dropping a table of that name if one exists), its indexes are created (ETL-7.01, with the field sizes of the new data, ETL-7.02), and then the data table is dropped (with its indexes and sequence) and the shadow table is renamed to the data table. The shadow table's indexes, primary key (`<dataTable>_pkey`) and sequence (`<dataTable>_rownr_seq`) are renamed to the names they would have if they had been created for the data table, so that repeated replaces do not collide with leftover names. The data table name stored in `datasets.data_table` never changes. A data table created before 2.0 with `rownr SERIAL` thereby becomes `BIGSERIAL` on its first replace. There is no `TRUNCATE` and no `RESTART IDENTITY`: row numbers of the new data start at 1. Before the data table is dropped, the shadow table gets the data table's owner (if it differs from the RowStore database user) and its table privileges (e.g., `SELECT` granted to a separate read-only user), which a new table would otherwise lack. If the data table has explicit privileges (a non-NULL ACL), they are copied exactly: all privileges on the shadow table are revoked (from `PUBLIC`, the owner and every other grantee) and each recorded privilege is granted, including the owner's own, so that privileges the owner revoked from itself stay revoked; with a NULL ACL, both tables have the default privileges. Table privileges are copied, column-level privileges are not; the copied privileges are granted by the owner, so the owner is recorded as their grantor. Until the commit, the previous data table and the shadow table (each with its indexes) coexist, so a replace temporarily needs disk space for both (in 1.x, the previous data was freed first). Objects created by operators that depend on a data table, e.g., views, make the `DROP TABLE` fail, so the replace ends in ERROR (keeping the previous data); RowStore itself creates no such objects.

> **ETL-6.03** Status semantics of a replace (as in 1.x): the dataset is PROCESSING (2) during the load, AVAILABLE (3) after the shadow table has replaced the data table, and ERROR (4) if the load fails for any reason (e.g., a malformed row, an SQL error, a lost database connection, an outage or a restart). In every case, queries, exports, the row count and the column names keep serving the previous data until the replacement is committed; a failed replace leaves the previous data unchanged (its uncommitted shadow table is discarded by the rollback). A dataset in ERROR after a failed replace therefore still serves its previous data. An append (POST) loads directly into the data table in one transaction; it is not visible before it is committed, and a failed append leaves the data unchanged as well.

> **ETL-6.04** Replacing the data table requires an `ACCESS EXCLUSIVE` lock on it, which has to wait for running readers (e.g., a long streaming export or a backup with `pg_dump` holds an `ACCESS SHARE` lock until it is finished), and while it waits, new queries of the data table queue behind it. Therefore the first four attempts to take the lock run with `SET LOCAL lock_timeout = '3s'` within a savepoint; if an attempt times out, the savepoint is rolled back, a WARN is logged, and the next attempt follows after 1, 2, 5 and 5 seconds, so that queries are not blocked for more than about 3 seconds at a time. The fifth and last attempt waits without a lock timeout (`lock_timeout = 0`), so that a replace always succeeds eventually, as in 1.x; if a reader holds the data table for longer, new queries of the dataset wait behind the replace until that reader has finished (as with `TRUNCATE` in 1.x). A `statement_timeout` configured for the RowStore database user also limits this wait (a timeout fails the load). The load's transaction is idle during the delays between attempts, so an `idle_in_transaction_session_timeout` configured for the RowStore database user must be longer than 5 seconds. After the lock has been acquired, the attempt's `lock_timeout` stays in force until the commit, which is harmless since the remaining statements only touch the locked data table, objects depending on it, and the shadow table. The dataset's further jobs wait meanwhile (ETL-8.02), and a replace waiting for the lock holds its ETL permit, so several replaces waiting behind one long reader (e.g., `pg_dump`) can use up `maxetlprocesses` and delay the loads of other datasets (as in 1.x). Queries that queue behind the last attempt are bounded by `querytimeout`, if configured (QUERY-7.01). With a `queryDatabase` read replica (CFG-5.01), the replayed `ACCESS EXCLUSIVE` lock conflicts with queries running on the replica, which PostgreSQL cancels after `max_standby_streaming_delay` (or delays the replay, depending on the replica's configuration).

> **ETL-6.05** A shadow table is never committed under its own name. Tables matching the shadow table pattern (DOM-6.02) that exist nevertheless are dropped at the start of the next replace of the dataset, when the dataset is deleted (API-3.07), and at startup (logged at WARN level, in the same startup step as the reset of ETL-8.06). Only tables matching the strict pattern are dropped.

## ETL-7 Auto-Indexing

> **ETL-7.01** After all rows are inserted, a `text_pattern_ops` index is created for each column on the JSONB-extracted text value:
> ```sql
> CREATE INDEX <table>_jsonidx_<md5> ON <table>
>     ((data->>'<column>') text_pattern_ops)
> ```
> Column names are escaped via `BaseConnection.escapeString()` (accessed via `Connection.unwrap()` through the HikariCP proxy) to prevent SQL injection in DDL.

> **ETL-7.02** Columns where any field value exceeds **256 characters** are not indexed. Field sizes are tracked per column during insertion using a `columnSize` HashMap. For columns renamed by the cleanup of column names (ETL-11.03), the field size is determined with `max(length(data->>'<column>'))` over the data table.

## ETL-8 Concurrency Control

> **ETL-8.01** [REMOVED] Busy-waiting (polling every 5 seconds while holding a permit) for a dataset in PROCESSING state. Superseded by per-dataset queues, see ETL-8.02 to ETL-8.07.

> **ETL-8.02** Jobs are queued per dataset (in memory). Jobs of the same dataset run one at a time, strictly in submission order, on a virtual thread that exists while the dataset has queued jobs. For example, a PUT followed by several append POSTs is applied in exactly that order. Datasets are identified by their canonical ID (the lowercase UUID as stored), so uploads via different spellings of the ID (e.g., upper case) or via an alias share one queue; responses keep the requested spelling. `EtlProcessor.runExclusively(dataset, task)` (and `callExclusively` for a task with a result) runs other work under the same per-dataset ordering and exclusion; it returns a future that is completed when the task has finished (exceptionally if the task failed) and cancelled if the task is discarded without running (ETL-8.07, shutdown).

> **ETL-8.03** A replace (PUT, no append) discards all loads of the same dataset that are queued but have not started, since it supersedes them. Their temp files are deleted and the discarding is logged at INFO level. A running load is never interrupted, and tasks submitted with `runExclusively` are not discarded.

> **ETL-8.04** A `Semaphore` permit is acquired only for the job that is next in line for its dataset, i.e., when no other job of its dataset is running. Jobs waiting behind another job of their dataset hold no permit and do not block loads of other datasets. While waiting for a permit, the next job stays in the queue and can still be discarded by a replace (ETL-8.03); the permit is held from just before that job is taken from the queue until it has finished, so a dataset never holds more than one permit. The total number of concurrent jobs stays limited by `maxetlprocesses`. `activeProcesses` (exposed as `activeEtlProcesses` by `/status` and the health indicator) counts running jobs only.

> **ETL-8.05** A database must not be used by more than one RowStore instance. Therefore a dataset in PROCESSING that is not being loaded by this instance is stale (e.g., left behind by a database outage) and is taken over by the next load without waiting: the transition to PROCESSING is a conditional `UPDATE datasets SET status = 2 WHERE id = ? AND status <> 2`; if no row is updated, a WARN is logged and the load proceeds.

> **ETL-8.06** At startup, all datasets in PROCESSING are set to ERROR, and their IDs are logged at WARN level, since their loads were interrupted (e.g., by a crash). Datasets in ACCEPTED_DATA are left unchanged, even if their queued load was lost by the restart or their status could not be updated at the start of a load because of a database outage (ETL-10.02); they are processed again by the next upload.

> **ETL-8.07** If a job fails abnormally with an `Error` (e.g., `OutOfMemoryError`, or an `AssertionError` thrown by a task), the error is logged, the remaining queued jobs of the dataset are discarded (temp files deleted), and the dataset's queue is removed, so that jobs submitted later for the dataset are processed by a new worker.

## ETL-9 Temp File Lifecycle

> **ETL-9.01** Uploaded CSV content is written to a temp file named `RowStore-*.csv` via `File.createTempFile()`. The file is marked with `deleteOnExit()` as a safety net. It is explicitly deleted after its load has finished, whether successful or not, when its queued load is discarded (ETL-8.03, ETL-8.07, shutdown), or on early failure (before queuing). Upload size is enforced by `maxuploadsize` (default 100 MB).

## ETL-10 Failure Handling

> **ETL-10.01** `populate()` tracks the outcome of a load locally and sets the final status (AVAILABLE on success, ERROR otherwise) from it, without reading the status back from the database. Exceptions that are not handled inside `populate()` (e.g., I/O errors, runtime exceptions) still set ERROR and then propagate to the `EtlProcessor`, which logs them with stack trace, makes a further best-effort attempt to set ERROR, and continues with the next job.

> **ETL-10.02** The final status write of a load is retried with backoff if it fails, e.g., because the database is unreachable: up to six attempts with delays of 1, 5, 15, 30 and 45 seconds, i.e., 96 seconds of delays plus the time each attempt needs to fail (up to the configured connect and login timeouts, 5 seconds each by default). During this time the job keeps its ETL permit and the dataset's further jobs wait. If all attempts fail, the dataset remains in its previous status: PROCESSING (taken over by the next load, ETL-8.05, or reset at the next startup, ETL-8.06), or ACCEPTED_DATA if the outage already prevented the transition to PROCESSING.

## ETL-11 Cleanup of Column Names Stored Before 2.0

> **ETL-11.01** After startup (Spring `ApplicationReadyEvent`), a background task on a virtual thread cleans the column names of existing datasets without delaying the startup. A dataset is affected if one of its column names (the keys of its first row, as returned by `/info`) is not clean, i.e., differs from its cleaned name (ETL-11.02); in practice these are names with a byte order mark or soft hyphens (ETL-4.06, ETL-4.07). Datasets in CREATED, datasets without a data table, and datasets whose data table does not exist are skipped. `PgRowStore.cleanColumnNames()` runs the cleanup and returns the IDs of the renamed datasets. There is no configuration option.

> **ETL-11.02** Cleaning rule for a stored column name: the name is to be the one that a fresh import of the same raw CSV header yields with the configured parser (`legacyparser`). In 1.x, the byte order mark in front of the first label changed how the label was parsed; the rule was derived from the behaviour of both parsers of OpenCSV 5.11.2 (as used by 1.x and 2.0) for a table of raw headers, which is pinned by unit tests (TC-ETL-039, TC-ETL-042). Only if the stored name starts with U+FEFF directly followed by a double quote, the remainder after the byte order mark is unquoted first:
> - **Default parser** (RFC 4180): 1.x kept the enclosing quotes but decoded doubled quotes (raw `<BOM>"kopare_id"` was stored as `<BOM>"kopare_id"`, `<BOM>"a""""b"` as `<BOM>"a""b"`, `<BOM>"""x"""` as `<BOM>""x""`). The enclosing quotes are removed without decoding doubled quotes again (`kopare_id`, `a""b`, `"x"`); a single quote (from raw `<BOM>""`) becomes empty.
> - **Legacy parser**: 1.x removed the quotes like 2.0, but kept one quote too many if the label started with a doubled quote inside the quotes (raw `<BOM>"""x"""` was stored as `<BOM>""x"`, `<BOM>""` as `<BOM>"`). The first quote is removed (`"x"`, empty).
> - **Plain quoted names** of the form `<BOM>"x..."` (at least one character between the quotes, the first of which is not a quote) are unquoted like by the default parser regardless of the configured parser: this is the common case of a plain quoted header stored by the default parser, also if `legacyparser: true` was configured after the dataset was loaded (`<BOM>"kopare_id"` becomes `kopare_id`), whereas the legacy parser stored such names only for unlikely headers (raw `<BOM>""x""`). For the default parser this is the rule above.
>
> Then, and for all other names, the name is normalized like a label (ETL-4.04, ETL-4.07): U+FEFF and U+00AD are removed anywhere, the ends are trimmed, and the name is converted to lowercase. Names without a leading byte order mark that merely look quoted are not unquoted (the header `"""x"""` legitimately yields `"x"`), and neither are names with whitespace between the byte order mark and the quote (raw `<BOM> "x"` yields `"x"` with the default parser, `x` with the legacy parser). The cleaned name of a clean name is the name itself. Residual ambiguity: a few stored names do not determine the raw header; they are cleaned to the name of the more likely header. `<BOM>"x"` is cleaned to `x` (plain quoted header), although raw `<BOM>""x""` yields `"x"` with the default and `x"` with the legacy parser. The quote-only names `<BOM>""` and `<BOM>"""` (from raw `<BOM>""""` and `<BOM>""""""` with the default parser) are cleaned to one quote less than 2.0 yields (`"` and `""`): an empty name skips the dataset (ETL-11.05), `"` is accepted. With the legacy parser, `<BOM>x"` is kept as `x"` (raw `<BOM>"x"""`), although raw `<BOM>"x" ` (space after the closing quote) yields `x`. Other names stored by 1.x with the other parser than the configured one, e.g., `<BOM>""x""`, are cleaned by the rule of the configured parser. An append resolves such cases by the labels it brings (ETL-11.04). A quoted first label that contains the separator was split into several columns by the default parser of 1.x; such datasets cannot be repaired by renaming. The rule is implemented once (`ColumnNames`) and shares the label normalization of ETL-4.04 and ETL-4.07.

> **ETL-11.03** Each affected dataset is renamed by a task that runs exclusively for the dataset (`EtlProcessor.callExclusively`, ETL-8.02), so it never runs concurrently with a load of the dataset and keeps its order with the dataset's jobs. Datasets are processed one at a time: the next dataset is submitted only after the previous one has finished, to limit the load on the database. Per dataset, in one transaction:
> 1. The column names are read again and the renames are determined (ETL-11.02, ETL-11.05).
> 2. A scan checks that no row contains both an old name and its new name (ETL-11.05).
> 3. All renamed keys of all rows are replaced in one `UPDATE`; rows without any of the keys are not touched, and a row without a key gets no new key.
> 4. The field sizes of the new names are determined in one scan that returns one row per column (`max(length(data->>k))` grouped by the column `k` of an array of the new names, so that the number of columns is not limited), and the indexes of the new names are created with the canonical naming (ETL-7.01) unless the size rule of ETL-7.02 applies. Index names only contain the first 8 hex digits of the MD5 of the column name, so the canonical name of an index of a new name may be taken by an index of an old name (e.g., the old name `field_<SHY>11246` and the new name `field_100652`); such an index is created under its canonical name with the suffix `_tmp`.
> 5. As the last statements before the commit, the indexes of the old names (named after the MD5 of the old name) are dropped, and indexes created with the suffix `_tmp` are renamed to their canonical name. Dropping an index locks the data table exclusively until the commit; the lock is acquired like for a replace (ETL-6.04): four attempts with a 3 s `lock_timeout` and short delays (a WARN per timed out attempt), then an unbounded wait, so that a long export does not make queries pile up behind the waiting lock for more than about 3 s at a time.
>
> Steps 1 to 4 do not block readers (`UPDATE` takes `ROW EXCLUSIVE`, `CREATE INDEX` `SHARE`): until the commit, queries, exports, `/info` and `/swagger` see the previous rows with the old names (MVCC), and the old indexes are still used. Only the final drop and the commit block readers briefly, or until a running reader has finished (ETL-6.04). The status is not changed. Cost: the `UPDATE` rewrites every row that has a renamed key, i.e., usually the whole data table, with maintenance of all its indexes; until `VACUUM` (autovacuum) reclaims the old row versions, the data table needs about twice its size, the write-ahead log grows by about the table size, and a `queryDatabase` read replica (CFG-5.01) may lag behind accordingly. The renames are logged at INFO level, with byte order marks and soft hyphens written as `\uFEFF` and `\u00AD`. A failure of a dataset is logged at ERROR level and the cleanup continues with the next dataset. The cleanup stops, logged once at WARN level, when RowStore shuts down or the database is not reachable (connection failures, SQL state class 08); the remaining datasets are cleaned at the next startup or by an append.

> **ETL-11.04** An append (POST) to an affected dataset that arrives before the cleanup has renamed it does not fail: each stored name that is not a label of the uploaded CSV is renamed to the label that it is cleaned to by the rule of either parser (ETL-11.02), if exactly one such label exists; the renamed names must equal the labels. Otherwise, or if a row contains both an old and a new name (ETL-11.05), the append fails with a column mismatch (ERROR) as before. The renames are applied as in ETL-11.03 within the append's transaction before the rows are inserted (logged at INFO level); the indexes of the old names are dropped after the indexes of the appended data have been created, as the last statements before the commit, so the append itself does not block readers. A failed append rolls the renames back as well.

> **ETL-11.05** If a cleaned name would be empty or would collide with another column name of the first row (an existing name or another cleaned name), or if any row of the data table contains both an old name and its new name (possible if the first row is shorter than others, e.g., 1.x stored `<BOM>name,name` with a first row that only has a value for the first column), the dataset is skipped with a WARN, and its data is not changed. Renaming would otherwise overwrite values. Such a dataset keeps its column names until it is replaced (PUT), which stores clean names; an append to it fails as before (column mismatch).

> **ETL-11.06** The cleanup is idempotent: a renamed dataset has clean names, so a repeated cleanup (e.g., at the next startup) finds nothing and changes no rows. A replace (PUT) of an affected dataset also yields clean names by itself (ETL-4.06, ETL-4.07). Affected column names change as a result (e.g., `<BOM>"kopare_id"` becomes `kopare_id`), so clients that queried such datasets by the broken name must use the clean name; this is to be mentioned in the release notes.

## Known Limitations

- No progress reporting during ETL processing (no percentage or row count updates)
- Separator detection only checks semicolon vs comma (no support for tab or pipe auto-detection)
- Batch size (200) is hardcoded, not configurable
- No retry mechanism for failed ETL jobs
- The ETL queue is held in memory only: queued loads are lost on restart and their datasets remain in ACCEPTED_DATA
- Column structure validation for append mode happens during processing, not at upload time
- The cleanup of column names (ETL-11) only considers the keys of the first row, like `/info`; a column whose key is missing in the first row (a short first row) is not detected

## References

- [Domain Model](02-domain-model.md#dom-4-physical-database-schema) — Entity model and database schema
- [REST API](03-api.md#api-3-endpoint-catalog) — Upload endpoints (POST /datasets, POST/PUT /dataset/{id})
- [Configuration](06-configuration.md#cfg-3-application-options) — `maxetlprocesses`, `legacyparser`, `maxuploadsize` options
- [Glossary](12-glossary.md#glo-1-terms) — ETL, Populate, Append Mode, Replace Mode
- Source: `EtlProcessor.java`, `PgDataset.java` (populate method), `DatasetUtil.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: virtual threads + Semaphore replace queue polling, HikariCP unwrap for BaseConnection |
| 2026-10-20 | Failure handling of loads (ETL-10) |
| 2026-10-20 | Per-dataset job queues with ordering and coalescing, permits only for running jobs, stale PROCESSING takeover and startup reset, abnormal worker failures (ETL-8.02 to ETL-8.07, ETL-8.01 removed) |
| 2026-10-20 | Atomic replace via shadow table, previous data served during and after a failed replace, swap lock timeout with retries, cleanup of leftover shadow tables (ETL-6.02 updated, ETL-6.03 to ETL-6.05) |
| 2026-10-20 | Empty lines in CSV files: trailing ones ignored, others fail the load, same for both parsers (ETL-4.05) |
| 2026-10-20 | Replace: last swap lock attempt waits without timeout, short delays between attempts, owner and privileges kept, disk usage and dependent objects documented (ETL-6.02, ETL-6.04) |
| 2026-10-20 | Replace: exact copy of explicit table privileges, ETL permit and querytimeout during the swap wait (ETL-6.02, ETL-6.04) |
| 2026-10-20 | Byte order mark skipped and labels sanitized (ETL-4.06, ETL-4.07, ETL-4.04 updated), cleanup of column names stored before 2.0 (ETL-11), future returned by runExclusively (ETL-8.02) |
| 2026-10-20 | Column name cleanup: parser-specific cleaning rule derived from the parsers, collisions checked in all rows, old indexes dropped last with the swap lock pattern, cost of the rewrite, stop on connection failures; trimming keeps 1.x behaviour for control characters (ETL-4.04, ETL-11.02 to ETL-11.05) |
| 2026-10-21 | Column name cleanup: plain quoted names unquoted regardless of the parser, size scan without a column limit, index name collisions between old and new names (ETL-11.02, ETL-11.03) |
