# ETL Processing Tests

## Overview

Tests for the scheduling and failure handling of ETL loads: per-dataset ordering, coalescing of queued loads,
permits only for running loads, takeover of stale PROCESSING datasets, startup reset, status handling when a
load fails or the database is unreachable, atomic replaces, and the cleanup of column names stored before 2.0.
See [04-etl-pipeline.md](../04-etl-pipeline.md) (ETL-6, ETL-8, ETL-10, ETL-11).

TC-ETL-001 to TC-ETL-009, TC-ETL-015 to TC-ETL-017, TC-ETL-027 and TC-ETL-028 are unit tests (`EtlProcessorTest`,
`PgDatasetPopulateTest`, `PgDatasetSwapLockTest`) that use fakes instead of a database. TC-ETL-010 to TC-ETL-014
and TC-ETL-018 are integration tests (`EtlProcessingIT`), TC-ETL-019 to TC-ETL-026 and TC-ETL-029 integration tests of
replaces (`DatasetReplaceIT`). TC-ETL-030 is a unit test (`EtlProcessorTest`), TC-ETL-031 to TC-ETL-037 are
integration tests of the column name cleanup (`ColumnNameCleanupIT`), and TC-ETL-038 to TC-ETL-041 unit tests of the
cleaning rule (`ColumnNamesTest`).

## Prerequisites

- Unit tests: none
- Integration tests: running RowStore instance; TC-ETL-010, TC-ETL-013, TC-ETL-014, TC-ETL-020 to TC-ETL-026 and TC-ETL-029
  additionally require direct database access (superuser) and are skipped when testing an external instance; so do
  TC-ETL-031 to TC-ETL-037, which also call into the RowStore application (TC-ETL-035 additionally starts separate
  RowStore instances on a separate database)

## Test Data

Generated CSV files with the header `name,value` and rows `<prefix><n>,<n>` (e.g., `A1,1`, `A2,2`).

## Scheduling (unit tests)

### TC-ETL-001: Loads of the same dataset run one at a time in submission order

**Steps:**
1. `maxetlprocesses = 5`. Submit load A1 of dataset A and keep it running
2. Submit loads A2, A3, A4 of dataset A
3. Release A1

**Assertions:**
1. A2 to A4 do not start while A1 is running
2. The loads start in the order A1, A2, A3, A4
3. At most one load of dataset A runs at any time

### TC-ETL-002: Loads of different datasets run concurrently up to the permit limit

**Steps:**
1. `maxetlprocesses = 2`. Submit loads A1, B1 of datasets A and B and keep them running
2. Submit load C1 of dataset C
3. Release A1

**Assertions:**
1. A1 and B1 run concurrently, C1 does not start while both are running, `activeEtlProcesses` is 2
2. C1 starts after A1 has finished

### TC-ETL-003: A load waiting for its dataset does not hold a permit

**Steps:**
1. `maxetlprocesses = 2`. Submit load A1 of dataset A and keep it running
2. Submit load A2 of dataset A, then load B1 of dataset B

**Assertions:**
1. B1 starts while A1 is running and A2 is waiting (A2 does not occupy the second permit)
2. `activeEtlProcesses` is 2 (A1 and B1; the waiting A2 is not counted)
3. A2 starts after A1 has finished

### TC-ETL-004: A replace discards waiting loads of the same dataset but not the running one

**Steps:**
1. Submit append load A1 of dataset A and append load B1 of dataset B, keep both running
2. Submit append load A2, replace load A3, append load B2, replace load A4

**Assertions:**
1. The temp files of A2 and A3 are deleted when the replaces are submitted; A2 and A3 never run
2. The running load A1 is not interrupted; the waiting load B2 of the other dataset is kept
3. A4 runs after A1; B2 runs after B1
4. All temp files are deleted once the loads have finished

### TC-ETL-005: Failing loads are logged with stack trace, set to error, and do not block subsequent loads

**Steps:**
1. `maxetlprocesses = 1`. Submit loads A1 (throws a runtime exception), A2 (throws an `IOException`), A3 (succeeds)
   of dataset A and load B1 of dataset B. The failing loads leave the dataset in PROCESSING

**Assertions:**
1. All loads run, A1 to A3 in submission order
2. The exceptions of A1 and A2 are logged at ERROR level with the exception (stack trace) attached
3. The status of dataset A is set to ERROR after A1 and A2, and to AVAILABLE after A3
4. No permit is leaked (B1 runs with a single permit, `activeEtlProcesses` returns to 0); all temp files are deleted

### TC-ETL-006: A task run exclusively for a dataset runs in order with the dataset's loads

**Steps:**
1. Submit load A1 of dataset A and keep it running
2. Run a task exclusively for dataset A, then submit replace load A2
3. Release A1

**Assertions:**
1. The task does not run while A1 is running
2. The order is A1, task, A2 (the task is not discarded by the replace)

## Status handling of populate (unit tests)

### TC-ETL-007: A non-SQL failure sets the status to error without reading it back and propagates

**Setup:** The database cannot be read from (status reads fail); status writes succeed.

**Steps:**
1. Populate a dataset from a CSV file that does not exist (non-SQL `IOException`)

**Assertions:**
1. `populate()` throws the original `NoSuchFileException`
2. The status writes are PROCESSING, then ERROR
3. The status is never read from the database

### TC-ETL-008: A failed error status write is retried until it succeeds

**Setup:** As TC-ETL-007; the first two writes of the ERROR status fail (database unreachable); short retry delays.

**Assertions:**
1. `populate()` throws the original exception
2. The ERROR status is written three times, the last attempt succeeds

### TC-ETL-009: Retrying a failed status write is bounded and does not mask the original exception

**Setup:** As TC-ETL-007; all writes of the ERROR status fail; two retry delays configured.

**Assertions:**
1. The ERROR status write is attempted exactly three times (initial attempt and two retries)
2. `populate()` still throws the original exception

### TC-ETL-027: A timed out lock for replacing the data table is retried within a savepoint, the last attempt without timeout

**Setup:** `PgDataset.lockDataTableForReplace()` with a fake connection whose `LOCK TABLE` statements fail with SQL
state `55P03` (lock timeout) while a lock timeout is set; three retry delays of 1 ms configured.

**Assertions:**
1. If two attempts fail, each attempt is `SAVEPOINT`, `SET LOCAL lock_timeout = '3s'`, `LOCK TABLE <dataTable> IN
   ACCESS EXCLUSIVE MODE`, a failed attempt is followed by `ROLLBACK TO SAVEPOINT`, and the third attempt succeeds
   (returns true)
2. If all attempts with a timeout fail, there are three of them (one per delay), each rolled back to its savepoint,
   and the last attempt is `SAVEPOINT`, `SET LOCAL lock_timeout = '0'`, `LOCK TABLE ...`, which waits and succeeds
   (returns true; there is no giving up on lock timeouts, ETL-6.04)

### TC-ETL-028: A lock for replacing the data table that fails for another reason is not retried

**Setup:** As TC-ETL-027; `LOCK TABLE` fails with SQL state `08006` (connection failure).

**Assertions:**
1. The `SQLException` is thrown after one attempt, without rolling back to the savepoint

### TC-ETL-015: Different spellings of the same dataset ID share one queue

**Steps:**
1. Submit load A1 for a dataset via its lowercase ID and keep it running
2. Submit load A2 via the upper case spelling of the ID and load A3 via an alias (same canonical ID)
3. Release A1

**Assertions:**
1. A2 and A3 do not start while A1 is running
2. The loads run in the order A1, A2, A3, never concurrently

### TC-ETL-016: A load waiting for a permit can still be discarded by a replace

**Steps:**
1. `maxetlprocesses = 1`. Submit load B1 of dataset B and keep it running (occupies the only permit)
2. Submit append load A1 of dataset A (next in line for A, waiting for a permit)
3. Submit replace load A2 of dataset A
4. Release B1

**Assertions:**
1. The temp file of A1 is deleted when A2 is submitted; A1 never runs
2. The loads run in the order B1, A2

### TC-ETL-017: An Error thrown by a job does not stop subsequent jobs of the dataset

**Steps:**
1. `maxetlprocesses = 1`. Submit load A1 of dataset A and keep it running
2. Run a task exclusively for dataset A that throws an `AssertionError`, then submit append load A2
3. Release A1; after the task has failed, submit load A3

**Assertions:**
1. A2, queued behind the failing task, is discarded (temp file deleted) and never runs
2. A3 runs (a new worker is started for dataset A); the order is A1, task, A3
3. No permit is leaked (`activeEtlProcesses` returns to 0)

## Integration tests

### TC-ETL-010: A dataset stuck in processing does not block a new upload

**Steps:**
1. Create a dataset from CSV `A` (2 rows) and wait until it is available
2. Set its status to PROCESSING (2) directly in the database (simulates a stale status left by an outage)
3. `PUT /dataset/{id}` with CSV `B` (3 rows)

**Assertions:**
1. The PUT returns `202`
2. The dataset becomes available (status 3) within the usual wait time
3. The dataset contains exactly the rows `B1`, `B2`, `B3`

### TC-ETL-011: Rapid uploads to the same dataset are applied in submission order

**Steps:**
1. Create a dataset from CSV `X` (1 row) and wait until it is available
2. Without waiting in between: `PUT` CSV `A` (20000 rows), `POST` CSV `B` (2 rows), `POST` CSV `C` (2 rows)

**Assertions:**
1. All uploads return `202`
2. Eventually the dataset is available and has 20004 rows
3. Ordered by row number, the rows are `A1` ... `A20000`, `B1`, `B2`, `C1`, `C2`

### TC-ETL-012: Of two rapid replaces of the same dataset the last one wins

**Steps:**
1. Create a dataset from CSV `X` (1 row) and wait until it is available
2. Without waiting in between: `PUT` CSV `A` (20000 rows), `PUT` CSV `B` (2 rows)

**Assertions:**
1. Eventually the dataset is available and contains exactly the rows `B1`, `B2`

### TC-ETL-013: A database outage during a load results in error status once the database is back

**Steps:**
1. Create a dataset from CSV `A` (2 rows) and wait until it is available
2. Lock the dataset's data table (`LOCK TABLE ... IN ACCESS EXCLUSIVE MODE`) in a separate transaction
3. `PUT /dataset/{id}` with CSV `B`; wait until the load's `LOCK TABLE ... IN ACCESS EXCLUSIVE MODE` (which
   precedes the replacement of the data table by the shadow table, ETL-6.04) waits for the lock
4. Simulate an outage: disallow connections to the database (`ALTER DATABASE ... ALLOW_CONNECTIONS false`, from
   the maintenance database) and terminate the loader's backend (`pg_terminate_backend`); release the lock
5. `GET /dataset/{id}/info` while connections are disallowed
6. Allow connections again

**Assertions:**
1. Step 5 returns `503`, not `404` (see API-6.05)
2. While connections are disallowed, the status in the database is still PROCESSING (2)
3. After connections are allowed again, the status becomes ERROR (4) within 30 seconds (the status write is
   retried, see ETL-10.02)

### TC-ETL-014: Datasets left in processing are reset to error at startup

**Steps:**
1. Create two datasets and wait until they are available
2. Set the status of the first one to PROCESSING (2) and of the second one to ACCEPTED_DATA (1) in the database
3. Invoke the startup reset (`PgRowStore.resetInterruptedLoads()`, called by the constructor at startup)

**Assertions:**
1. The returned IDs contain the first dataset but not the second one
2. The first dataset has status ERROR (4), the second one still ACCEPTED_DATA (1)

### TC-ETL-018: Uploads via different spellings of a dataset ID and via an alias are applied in submission order

**Steps:**
1. Create a dataset from CSV `X` (1 row), wait until it is available, and set an alias for it
2. Without waiting in between: `PUT` CSV `A` (20000 rows) via the lowercase ID, `POST` CSV `B` (2 rows) via the
   upper case ID, `POST` CSV `C` (2 rows) via the alias

**Assertions:**
1. All uploads return `202`; the response to the upper case request contains the ID in upper case (as requested)
2. Eventually the dataset is available and has 20004 rows
3. Ordered by row number, the last rows are `A20000`, `B1`, `B2`, `C1`, `C2`

## Integration tests of replaces

### TC-ETL-019: A replace that fails on a corrupt row late in the file keeps the previous data

**Steps:**
1. Create a dataset from CSV `A` (3 rows) and wait until it is available
2. `PUT /dataset/{id}` with CSV `B` (1000 rows) followed by a row with more values than columns

**Assertions:**
1. The status becomes ERROR (4)
2. Queries return the rows `A1`, `A2`, `A3`; `/info` reports `rowcount` 3

### TC-ETL-020: A replace whose database connection is lost keeps the previous data

**Steps:**
1. Create a dataset from CSV `A` (3 rows) and wait until it is available
2. In a separate transaction, create the dataset's shadow table `<dataTable>_new` without committing, so that the
   load's `CREATE TABLE IF NOT EXISTS <dataTable>_new` waits for this transaction
3. `PUT /dataset/{id}` with CSV `B`; wait until the load's `CREATE TABLE` waits for a lock
4. Terminate the loader's backend (`pg_terminate_backend`) and roll back the separate transaction
5. `PUT /dataset/{id}` with CSV `C` (2 rows)

**Assertions:**
1. After step 4, the status becomes ERROR (4) and queries return the rows `A1`, `A2`, `A3`
2. After step 5, the dataset becomes available with the rows `C1`, `C2` (nothing is left behind)

### TC-ETL-021: Queries and exports during a replace return the previous data

**Steps:**
1. Create a dataset from CSV `A` (3 rows) and wait until it is available
2. Block the creation of the shadow table as in TC-ETL-020, `PUT /dataset/{id}` with CSV `B` (2 rows), and wait
   until the load's `CREATE TABLE` waits for a lock
3. Query the dataset, its info and its JSON export
4. Roll back the separate transaction

**Assertions:**
1. In step 3, the status is PROCESSING (2), queries and the export return the rows `A1`, `A2`, `A3`, and `rowcount`
   is 3
2. After step 4, the dataset becomes available with the rows `B1`, `B2`

### TC-ETL-022: Consecutive replaces succeed and leave the data table with canonical names

**Steps:**
1. Create a dataset from CSV `A` and wait until it is available
2. Replace its data table directly in the database by a table as created before 2.0
   (`rownr SERIAL PRIMARY KEY, data JSONB NOT NULL`) with one row
3. Three times: `PUT /dataset/{id}` with CSV `B`, `C`, `D` (2 rows each) and wait until it is available

**Assertions:**
1. After each replace, queries return exactly the rows of the last CSV
2. Only the table `<dataTable>` exists (no `<dataTable>_new`); its indexes are exactly `<dataTable>_pkey`,
   `<dataTable>_jsonidx_<md5("name")[0..8]>` and `<dataTable>_jsonidx_<md5("value")[0..8]>`; its primary key
   constraint is `<dataTable>_pkey`; its serial sequence is `public.<dataTable>_rownr_seq`
3. The column `rownr` and its sequence are `bigint`

### TC-ETL-023: A replace waits for a reader of the data table, e.g., an export, and then succeeds

**Steps:**
1. Create a dataset from CSV `A` (3 rows) and wait until it is available
2. In a separate transaction, read the data table (holds an `ACCESS SHARE` lock like a streaming export)
3. `PUT /dataset/{id}` with CSV `B` (2 rows); wait until the load's `LOCK TABLE <dataTable> IN ACCESS EXCLUSIVE MODE`
   waits for the lock, then wait 5 seconds (longer than the lock timeout of an attempt, 3 seconds)
4. End the separate transaction

**Assertions:**
1. In step 3, after the first attempt has timed out, the status in the database is still PROCESSING (2) and queries
   return the rows `A1`, `A2`, `A3`
2. After step 4, the dataset becomes available with the rows `B1`, `B2`

### TC-ETL-024: A leftover shadow table is dropped at the start of a replace

**Steps:**
1. Create a dataset from CSV `A` (3 rows) and wait until it is available
2. Create and commit a table `<dataTable>_new` with one row `leftover`
3. `PUT /dataset/{id}` with CSV `B` (2 rows)

**Assertions:**
1. The dataset becomes available with exactly the rows `B1`, `B2` (the leftover row is not loaded)
2. No table `<dataTable>_new` exists

### TC-ETL-025: Leftover shadow tables are dropped at startup

**Steps:**
1. Create a dataset from CSV `A` (3 rows) and wait until it is available
2. Create and commit the tables `<dataTable>_new` and `<dataTable>_newer`
3. Invoke the startup cleanup (`PgRowStore.dropLeftoverShadowTables()`, called by the constructor at startup)

**Assertions:**
1. The returned table names contain `<dataTable>_new`, but neither `<dataTable>_newer` nor `<dataTable>`
2. `<dataTable>_new` is dropped, `<dataTable>_newer` still exists, and the dataset still returns its rows

### TC-ETL-026: Deleting a dataset drops a leftover shadow table

**Steps:**
1. Create a dataset and wait until it is available
2. Create and commit a table `<dataTable>_new`
3. `DELETE /dataset/{id}`

**Assertions:**
1. The response is `204` and the dataset is gone (`404`)
2. Neither `<dataTable>` nor `<dataTable>_new` exists

### TC-ETL-029: A replace keeps the owner and the privileges of the data table

**Steps:**
1. Create a dataset from CSV `A` (3 rows) and wait until it is available
2. Create the roles `rowstore_reader_<x>` and `rowstore_owner_<x>`; make the owner role the owner of the data table,
   grant `SELECT` on it to the reader role and `INSERT` to `PUBLIC`, and revoke `DELETE` from the owner role
3. `PUT /dataset/{id}` with CSV `B` (2 rows) and wait until it is available
4. Delete the dataset and drop the roles

**Assertions:**
1. Queries return the rows `B1`, `B2`
2. The data table is owned by the owner role; the reader role has `SELECT` but not `UPDATE`
   (`has_table_privilege`); the owner role has `SELECT` but still not `DELETE`; `PUBLIC` has `INSERT`

## Exclusive tasks (unit test)

### TC-ETL-030: The future of an exclusive task completes when the task has run, exceptionally if it failed, and is cancelled if the task is discarded

**Steps:**
1. Submit load A1 of dataset A and keep it running
2. Run four tasks exclusively for dataset A: one that succeeds, one that throws an `IllegalStateException`, one that
   throws an `AssertionError`, and one that succeeds
3. Release A1

**Assertions:**
1. The future of the first task is not done while A1 is running, and completes normally afterwards
2. The futures of the second and third task complete exceptionally with the thrown exception as cause
3. The fourth task is discarded because the worker failed with the error (ETL-8.07): its future is cancelled and the
   task does not run

## Cleanup of column names stored before 2.0 (integration tests)

`<BOM>` denotes U+FEFF and `<SHY>` U+00AD. The test datasets are created from
`kopare_id,namn,ort\n1,Anna,Göteborg\n2,Bo,Mölndal\n`. A dataset stored by 1.x is simulated by rewriting the keys of
all rows via JDBC and renaming the indexes of the renamed keys to the canonical names of the broken names
(`<dataTable>_jsonidx_<md5(name)[0..8]>`), by default `kopare_id` to `<BOM>"kopare_id"` and `namn` to `na<SHY>mn`
("broken names"). "Clean" means: the keys of the rows are exactly `kopare_id`, `namn`, `ort`, the `_jsonidx_` indexes
of the data table are exactly the canonical ones for these names, and the first two rows are unchanged apart from the
names.

### TC-ETL-031: The cleanup renames broken column names and their indexes and keeps the data and the status

**Steps:**
1. Create a dataset, give it broken names
2. Invoke the cleanup (`PgRowStore.cleanColumnNames()`, also run in the background after startup)

**Assertions:**
1. The returned IDs contain the dataset
2. The status is still AVAILABLE and `rowcount` = 2
3. The dataset is clean; `columnnames` are the clean names; `?kopare_id=1` and `?namn=Bo` return the expected rows

### TC-ETL-032: A repeated cleanup changes nothing

**Steps:**
1. Create a dataset, give it broken names, invoke the cleanup
2. Invoke the cleanup again

**Assertions:**
1. The second run does not return the dataset
2. No row was updated by the second run (`xmin` of all rows unchanged) and the dataset is clean

### TC-ETL-033: A dataset whose cleaned column names would collide is skipped, other datasets are cleaned

**Steps:**
1. Create two datasets; rename `ort` of the first one to `<BOM>"namn"` (whose cleaned name `namn` exists) and give
   the second one broken names
2. Invoke the cleanup

**Assertions:**
1. The returned IDs contain the second dataset but not the first; a WARN is logged for the first
2. The first dataset is unchanged: no row updated, keys and index names as before, status AVAILABLE
3. The second dataset is clean

### TC-ETL-034: An append to a dataset with broken column names renames the columns with the append

**Steps:**
1. Create a dataset, give it broken names
2. `POST /dataset/{id}` with `<BOM>"kopare_id",namn,ort\n3,Cecilia,Partille\n` (before any cleanup)

**Assertions:**
1. The status becomes AVAILABLE (3) and `rowcount` = 3 (without ETL-11.04: ERROR because of a column mismatch)
2. The dataset is clean, queries by the clean names return the old and the appended rows
3. A subsequent cleanup does not return the dataset

### TC-ETL-035: The cleanup runs in the background after startup

**Steps:**
1. Create a separate database and start a RowStore instance on it; create a dataset and wait until it is available;
   stop the instance
2. Give the dataset broken names via JDBC
3. Start another RowStore instance on the database

**Assertions:**
1. Within 30 seconds after the start, the dataset is clean (without any further request)

### TC-ETL-036: A dataset whose data table is missing is skipped without failing the cleanup

**Steps:**
1. Create two datasets; drop the data table of the first one and give the second one broken names
2. Invoke the cleanup

**Assertions:**
1. The cleanup does not fail; the returned IDs contain the second dataset but not the first
2. The second dataset is clean

### TC-ETL-037: The cleanup creates no index for a renamed column with values longer than 256 characters

**Steps:**
1. Create a dataset from `kopare_id,beskrivning\n1,<300 characters>\n2,kort\n` (only `kopare_id` is indexed, ETL-7.02)
2. Rename `beskrivning` to `<BOM>"beskrivning"`, invoke the cleanup

**Assertions:**
1. The keys are `kopare_id`, `beskrivning`; the only `_jsonidx_` index is the one of `kopare_id`
2. `?beskrivning=kort` returns the second row

## Cleaning rule (unit tests)

Unit tests of `ColumnNames` (`ColumnNamesTest`), no RowStore instance needed (ETL-4.04, ETL-4.07, ETL-11.02).

### TC-ETL-038: A label is normalized by removing byte order marks and soft hyphens, stripping and lower-casing

Cases include surrounding spaces and tabs, U+FEFF and U+00AD at any position, Unicode whitespace (U+2003), a label
consisting of whitespace and a byte order mark only (empty), and `<BOM>"name"` (quotes kept: unquoting is the
parser's task).

### TC-ETL-039: A stored column name is cleaned like the label is parsed now

| Stored name | Cleaned name |
|-------------|--------------|
| `<BOM>"kopare_id"` | `kopare_id` |
| `<BOM>kopare_id` | `kopare_id` |
| `<BOM>"a""b"` | `a"b` |
| `<BOM>"""x"""` | `"x"` |
| `<BOM>" x "` | `x` |
| `<BOM>"ko<SHY>pare"` | `kopare` |
| `<BOM>"` | `"` |
| `<BOM>""` | (empty) |
| `na<SHY>mn`, `<SHY>namn`, `<U+2003>namn` | `namn` |
| `"x"`, `<SHY>"x"`, `"x"<BOM>` | `"x"` (no leading byte order mark: not unquoted) |
| `kopare_id`, `a b`, (empty) | unchanged |

For each case, cleaning the cleaned name again yields the same name.

### TC-ETL-040: Renames are determined for unclean names only, and not at all if clean names would collide or be empty

Clean names yield no renames; `<BOM>"kopare_id"`, `na<SHY>mn`, `ort` yield the renames of the first two only;
`<BOM>"namn"` with `namn`, `<BOM>namn` with `na<SHY>mn`, and `<BOM>` (empty cleaned name) yield no result (skip).

### TC-ETL-041: Byte order marks and soft hyphens are escaped for logging

`<BOM>"na<SHY>mn"` is logged as `\uFEFF"na\u00ADmn"`.
