# ETL Processing Tests

## Overview

Tests for the scheduling and failure handling of ETL loads: per-dataset ordering, coalescing of queued loads,
permits only for running loads, takeover of stale PROCESSING datasets, startup reset, status handling when a
load fails or the database is unreachable, and atomic replaces. See [04-etl-pipeline.md](../04-etl-pipeline.md)
(ETL-6, ETL-8, ETL-10).

TC-ETL-001 to TC-ETL-009, TC-ETL-015 to TC-ETL-017, TC-ETL-027 and TC-ETL-028 are unit tests (`EtlProcessorTest`,
`PgDatasetPopulateTest`, `PgDatasetSwapLockTest`) that use fakes instead of a database. TC-ETL-010 to TC-ETL-014
and TC-ETL-018 are integration tests (`EtlProcessingIT`), TC-ETL-019 to TC-ETL-026 integration tests of replaces
(`DatasetReplaceIT`).

## Prerequisites

- Unit tests: none
- Integration tests: running RowStore instance; TC-ETL-010, TC-ETL-013, TC-ETL-014 and TC-ETL-020 to TC-ETL-026
  additionally require direct database access (superuser) and are skipped when testing an external instance

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

### TC-ETL-027: A timed out lock for replacing the data table is retried within a savepoint, bounded

**Setup:** `PgDataset.lockDataTableForReplace()` with a fake connection whose `LOCK TABLE` statements fail with SQL
state `55P03` (lock timeout); three retry delays of 1 ms configured.

**Assertions:**
1. If two attempts fail, each attempt is `SAVEPOINT`, `SET LOCAL lock_timeout = '3s'`, `LOCK TABLE <dataTable> IN
   ACCESS EXCLUSIVE MODE`, a failed attempt is followed by `ROLLBACK TO SAVEPOINT`, and the third attempt succeeds
   (returns true)
2. If all attempts fail, the lock is attempted four times (initial attempt and three retries), each failure is
   rolled back to its savepoint, and false is returned (the load then fails, ETL-6.04)

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
