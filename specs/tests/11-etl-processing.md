# ETL Processing Tests

## Overview

Tests for the scheduling and failure handling of ETL loads: per-dataset ordering, coalescing of queued loads,
permits only for running loads, shutdown, takeover of stale PROCESSING datasets, startup reset, status handling when a
load fails or the database is unreachable, atomic replaces, and the cleanup of column names stored before 2.0.
See [04-etl-pipeline.md](../04-etl-pipeline.md) (ETL-6, ETL-8, ETL-10, ETL-11).

TC-ETL-001 to TC-ETL-009, TC-ETL-015 to TC-ETL-017, TC-ETL-027, TC-ETL-028, TC-ETL-051, TC-ETL-054 and TC-ETL-055 are unit tests (`EtlProcessorTest`,
`PgDatasetPopulateTest`, `PgDatasetSwapLockTest`) that use fakes instead of a database. TC-ETL-010 to TC-ETL-014
and TC-ETL-018 are integration tests (`EtlProcessingIT`), TC-ETL-019 to TC-ETL-026, TC-ETL-029 and TC-ETL-053 integration tests of
replaces (`DatasetReplaceIT`). TC-ETL-030, TC-ETL-070 and TC-ETL-071 are unit tests (`EtlProcessorTest`), TC-ETL-031 to TC-ETL-037,
TC-ETL-045, TC-ETL-046, TC-ETL-049, TC-ETL-061 and TC-ETL-062 are integration tests of the column name cleanup
(`ColumnNameCleanupIT`), TC-ETL-038 to TC-ETL-044, TC-ETL-048 and TC-ETL-060 unit tests of the cleaning rule
(`ColumnNamesTest`), and TC-ETL-047 a unit test of the cleanup's stop condition (`PgRowStoreConnectionFailureTest`).

## Prerequisites

- Unit tests: none
- Integration tests: running RowStore instance; TC-ETL-010, TC-ETL-013, TC-ETL-014, TC-ETL-020 to TC-ETL-026, TC-ETL-029 and TC-ETL-053
  additionally require direct database access (superuser) and are skipped when testing an external instance; so do
  TC-ETL-031 to TC-ETL-037, TC-ETL-045, TC-ETL-046, TC-ETL-049, TC-ETL-061 and TC-ETL-062, which also call into the RowStore application (TC-ETL-035 additionally starts separate
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
2. `activeEtlProcesses` is 2 (A1 and B1; the waiting A2 is not counted), and `queuedEtlProcesses` is 1 (A2)
3. A2 starts after A1 has finished, and `queuedEtlProcesses` is 0 once all loads have finished

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

### TC-ETL-027: A timed out lock for replacing the data table is retried within a savepoint, every attempt with a short timeout

**Setup:** `PgDataset.lockDataTableForReplace()` with a fake connection whose `LOCK TABLE` statements fail with SQL
state `55P03` (lock timeout) while a lock timeout is set; three retry delays of 1 ms configured, a wait limit of
60 s, a maximum delay of 2 ms and a log interval of 60 s for the lock holders.

**Assertions:**
1. If two attempts fail, the statements are `SET LOCAL idle_in_transaction_session_timeout = 0` (once, before the first
   savepoint), then per attempt `SAVEPOINT`, `SET LOCAL lock_timeout = '3s'`, `LOCK TABLE <dataTable> IN
   ACCESS EXCLUSIVE MODE`, a failed attempt is followed by `ROLLBACK TO SAVEPOINT`, and the third attempt succeeds
   (returns true)
2. If five attempts fail (more than the configured delays), every attempt, including the further ones, runs with
   `SET LOCAL lock_timeout = '3s'`, each failed one is rolled back to its savepoint, the lock holders are queried
   (within a savepoint) exactly once, and the sixth attempt succeeds (returns true)

### TC-ETL-051: If the lock cannot be acquired within the wait limit, locking gives up and logs the lock holders

**Setup:** As TC-ETL-027, with a wait limit of 200 ms and a maximum delay of 5 ms; all `LOCK TABLE` statements fail
with SQL state `55P03`.

**Assertions:**
1. `lockDataTableForReplace()` returns false (the caller rolls back, ETL-6.04), after more than four `LOCK TABLE`
   attempts, all with `lock_timeout = '3s'`
2. The lock holders are queried twice: when the configured delays are used up, and after the last attempt was rolled
   back to its savepoint

### TC-ETL-028: A lock for replacing the data table that fails for another reason is not retried

**Setup:** As TC-ETL-027; `LOCK TABLE` fails with SQL state `08006` (connection failure).

**Assertions:**
1. The `SQLException` is thrown after one attempt, without rolling back to the savepoint

### TC-ETL-054: While the lock is retried, the lock holders are logged at most once per interval

**Setup:** As TC-ETL-027, with a log interval of 0 for the lock holders; five `LOCK TABLE` attempts fail.

**Assertions:**
1. The lock holders are queried after each of the three failed attempts that follow when the configured delays are
   used up; the sixth attempt succeeds

### TC-ETL-055: The delay between attempts to lock the data table grows up to 30 seconds

**Setup:** `PgDataset` with the default delays (1, 2, 5, 5 s) and the default maximum delay (30 s).

**Assertions:**
1. The delays after the attempts 1, 4, 5, 6, 7 and 101 are 1, 5, 10, 20, 30 and 30 seconds

### TC-ETL-050: A load of a dataset that was deleted while the load was queued is skipped

**Setup:** As TC-ETL-007, but the dataset does not exist in the database anymore: the conditional transition to PROCESSING updates no row.

**Assertions:**
1. `populate()` returns false without reading the CSV file
2. No status is written apart from the attempted transition to PROCESSING (in particular no ERROR)

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

### TC-ETL-070: Shutdown discards queued loads and logs their datasets, running loads finish

**Steps:**
1. With 1 permit, start a blocking load `a1` of dataset `a`
2. Submit `a2` to `a` (waits behind `a1`), `b1` to `b` (waits for the permit) and an exclusive task for `a`
3. Shut the ETL processor down in the background, then release `a1`

**Assertions:**
1. The files of `a2` and `b1` are deleted and the task's future is cancelled before `a1` is released
2. Only `a1` runs and ends with status AVAILABLE; `b` gets no status change
3. A WARN names both datasets as having discarded uploads (DEPL-7.01)
4. The shutdown returns once `a1` has finished

### TC-ETL-071: Loads submitted after shutdown are rejected and their files deleted

**Steps:**
1. Shut the ETL processor down
2. Submit a load and an exclusive task

**Assertions:**
1. Both are rejected with `RejectedExecutionException`
2. The load's file is deleted and nothing runs

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

### TC-ETL-014: Datasets left in processing or accepted data are reset to error at startup

**Steps:**
1. Create two datasets and wait until they are available
2. Set the status of the first one to PROCESSING (2) and of the second one to ACCEPTED_DATA (1) in the database
3. Invoke the startup reset (`PgRowStore.resetInterruptedLoads()`, called by the constructor at startup)

**Assertions:**
1. The returned IDs contain both datasets
2. Both datasets have status ERROR (4) (ETL-8.06: the queued upload of a dataset in ACCEPTED_DATA was lost when
   RowStore stopped)

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
4. `pg_stats` has statistics for the column `data` of `<dataTable>` (the shadow table is analyzed before the swap)

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

### TC-ETL-053: While a replace waits for a long reader, queries of the dataset are blocked for at most one short lock attempt

**Steps:**
1. Create dataset A from CSV `A` (3 rows) and dataset C from CSV `C` (1 row), and wait until they are available
2. In a separate transaction, read the data table of A (holds an `ACCESS SHARE` lock like a long export or `pg_dump`)
3. `PUT /dataset/{A}` with CSV `B` (2 rows); wait until the load's `LOCK TABLE` waits for the lock, then wait
   28 seconds (longer than the first four attempts with their delays, about 25 seconds)
4. For 12 seconds, query A and C repeatedly
5. End the separate transaction

**Assertions:**
1. In step 4, every query of A returns 200 with the rows `A1`, `A2`, `A3` within less than 5 seconds (at most one
   attempt's lock timeout of 3 seconds), every query of C returns 200 with `C1`, and the status of A is still
   PROCESSING (2)
2. After step 5, A becomes available with the rows `B1`, `B2` within 60 seconds (the next attempt follows within the
   maximum delay of 30 seconds)

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

### TC-ETL-035: The cleanup runs in the background after startup until a run has completed

**Steps:**
1. Create a separate database and start a RowStore instance on it; create a dataset and wait until it is available;
   stop the instance
2. Give the dataset broken names via JDBC and delete the rows of `maintenance_tasks` (like a database of 1.x)
3. Start another RowStore instance on the database and stop it
4. Give the dataset broken names again and start a third instance

**Assertions:**
1. After step 1, `maintenance_tasks` contains `column-name-cleanup` (the first start found nothing to clean)
2. Within 30 seconds after the start in step 3, the dataset is clean (without any further request) and the cleanup is
   recorded as completed again
3. In step 4, `startColumnNameCleanup()` returns null (skipped, ETL-11.07) and the broken names remain

### TC-ETL-052: A cleanup in which a dataset fails is not recorded as completed

**Steps:**
1. Delete the rows of `maintenance_tasks`; create a dataset and rename the column `data` of its data table, so that
   reading its column names fails with an SQL error that is not a connection failure
2. Run `cleanColumnNames()`
3. Restore the column and run `cleanColumnNames()` again

**Assertions:**
1. After step 2, the cleanup is not recorded as completed
2. After step 3, it is recorded as completed (`column-name-cleanup` in `maintenance_tasks`)

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

### TC-ETL-045: A dataset with a row that contains both an old and a new name is skipped by the cleanup and fails an append

**Steps:**
1. Create a dataset from `namn,ort\nfirst,1\nleft,2\n`; via JDBC, set row 1 to `{<BOM>namn: first}` and row 2 to
   `{<BOM>namn: left, namn: right}` (as stored by 1.x for `<BOM>namn,namn\nfirst\nleft,right`: the first row is short)
2. Invoke the cleanup
3. `POST /dataset/{id}` with `namn\nnew\n`

**Assertions:**
1. The cleanup does not return the dataset and updates no row (`xmin` unchanged); a WARN is logged
2. The append ends in ERROR (column mismatch, logged at ERROR level) and the rows are unchanged
3. Without the check of all rows, the cleanup renamed `<BOM>namn` to `namn` and overwrote `right` with `left`

### TC-ETL-046: Queries are served while the cleanup waits for a reader of the data table to drop the old indexes

**Steps:**
1. Create a dataset, give it broken names
2. Open a transaction that reads the data table (holds an `ACCESS SHARE` lock like a streaming export)
3. Invoke the cleanup in the background; wait until it waits for `LOCK TABLE <dataTable> IN ACCESS EXCLUSIVE MODE`,
   then 4 more seconds (the first lock attempt times out)
4. `GET /dataset/{id}?ort=Göteborg`
5. End the reading transaction

**Assertions:**
1. The query of step 4 returns within 10 seconds the previous row with the old names (the renames are not committed
   yet), and the cleanup has not finished
2. After step 5, the cleanup returns the dataset and the dataset is clean
3. Without the lock pattern of ETL-11.03 (old indexes dropped right after the `UPDATE`), the cleanup waits in
   `DROP INDEX` instead and queries queue behind it until the reader has finished

## Cleaning rule (unit tests)

Unit tests of `ColumnNames` (`ColumnNamesTest`), no RowStore instance needed (ETL-4.04, ETL-4.07, ETL-11.02, ETL-11.04,
ETL-11.08, ETL-11.09).

### TC-ETL-038: A label is normalized by removing byte order marks and soft hyphens, trimming and lower-casing

Cases include surrounding spaces and tabs, U+FEFF and U+00AD at any position, Unicode whitespace (U+2003), control
characters at the ends (U+0001, removed like by `trim()` in 1.x, also next to Unicode whitespace) and inside (kept),
no-break spaces (U+00A0, kept), a label consisting of whitespace and a byte order mark only (empty), and `<BOM>"name"`
(quotes kept: unquoting is the parser's task).

### TC-ETL-039: A stored column name is cleaned to the name a fresh import of the same header yields with the configured parser

For each raw first label and parser, the test determines with the actual OpenCSV parsers (configured as in
`PgDataset`) the name stored by 1.x (`<BOM>` + label parsed, trimmed, lower-cased) and the name of a fresh import by
2.0 (label parsed without byte order mark, normalized), asserts both against the table, and asserts that cleaning the
stored name with that parser yields the 2.0 name, and that cleaning it again changes nothing. The labels of the table
have no surrounding Unicode whitespace, for which the cleaned name differs from a fresh import (ETL-11.08).

| Parser | Raw first label | Stored by 1.x | Name in 2.0 |
|--------|-----------------|---------------|-------------|
| default | `"kopare_id"`, `"Kopare_ID"` | `<BOM>"kopare_id"` | `kopare_id` |
| default | `kopare_id` | `<BOM>kopare_id` | `kopare_id` |
| default | `"a ""b"""` | `<BOM>"a "b""` | `a "b"` |
| default | `"a""""b"` | `<BOM>"a""b"` | `a""b` |
| default | `"a""b"` | `<BOM>"a"b"` | `a"b` |
| default | `"""x"""` | `<BOM>""x""` | `"x"` |
| default | `"""x"` | `<BOM>""x"` | `"x` |
| default | `"""a""b"""` | `<BOM>""a"b""` | `"a"b"` |
| default | `"x"""` | `<BOM>"x""` | `x"` |
| default | `" x "` | `<BOM>" x "` | `x` |
| default | `"ko<SHY>pare"` | `<BOM>"ko<SHY>pare"` | `kopare` |
| default | ` "x"` | `<BOM> "x"` | `"x"` |
| default | `x"` | `<BOM>x"` | `x"` |
| default | `""` | `<BOM>"` | (empty) |
| legacy | `"kopare_id"`, `kopare_id` | `<BOM>kopare_id` | `kopare_id` |
| legacy | `"a ""b"""` | `<BOM>a "b"` | `a "b"` |
| legacy | `"a""""b"` | `<BOM>a""b` | `a""b` |
| legacy | `"a""b"` | `<BOM>a"b` | `a"b` |
| legacy | `"""x"""` | `<BOM>""x"` | `"x"` |
| legacy | `"""x"` | `<BOM>""x` | `"x` |
| legacy | `"""a""b"""` | `<BOM>""a"b"` | `"a"b"` |
| legacy | `""""` | `<BOM>""` | `"` |
| legacy | `""""""` | `<BOM>"""` | `""` |
| legacy | `"x"""` | `<BOM>x"` | `x"` |
| legacy | ` "x"`, `" x "` | `<BOM> x` | `x` |
| legacy | `""` | `<BOM>"` | (empty) |

### TC-ETL-040: Renames are determined for unclean names only, not for names that clean to empty, and not at all if clean names would collide

Clean names yield no renames; `<BOM>"kopare_id"`, `na<SHY>mn`, `ort` yield the renames of the first two only (default
parser); `<BOM>kopare_id` is renamed with the legacy parser; `<BOM>"namn"` with `namn` and `<BOM>namn` with
`na<SHY>mn` yield no result (skip). `<BOM>"` with `namn` yields no renames, and `<BOM>` with `na<SHY>mn` only the
rename of `na<SHY>mn` (the name that cleans to empty is kept, ETL-11.09). `namn `, ` ort`, `zon<U+2003>` and `Kommun`
yield no renames (ETL-11.08).

### TC-ETL-041: Byte order marks and soft hyphens are escaped for logging

`<BOM>"na<SHY>mn"` is logged as `\uFEFF"na\u00ADmn"`.

### TC-ETL-042: Ambiguous stored column names are cleaned to the name of the more likely header

Verified with the actual parsers as in TC-ETL-039, for the less likely raw header. `<BOM>"x"` stems from the plain
quoted header `"x"` stored by the default parser in the common case. For the quote-only names, no header yields the
cleaned name: the cleaned name has one quote less than the name of the header in 2.0; the empty name is kept as it
is (ETL-11.09), `"` is accepted.

| Parser | Less likely raw first label | Stored by 1.x | Cleaned name | Name of that header in 2.0 |
|--------|-----------------------------|---------------|--------------|----------------------------|
| default | `""x""` | `<BOM>"x"` | `x` | `"x"` |
| legacy | `""x""` | `<BOM>"x"` | `x` | `x"` |
| default | `""""` | `<BOM>""` | (empty) | `"` |
| default | `""""""` | `<BOM>"""` | `"` | `""` |
| legacy | `"x" ` | `<BOM>x"` | `x"` | `x` |

### TC-ETL-043: Stored column names without a byte order mark directly followed by a quote are only cleaned of byte order marks and soft hyphens

With both parsers: `na<SHY>mn`, `<SHY>namn` and `<SHY> namn` (trimmed like by 1.x once the soft hyphen is removed)
become `namn`; names without U+FEFF and U+00AD are unchanged (ETL-11.08): `<U+2003>namn`, `<U+0001>namn`, `namn `,
`Namn`, `kopare_id`, `a b` and the empty name; `<U+2003>na<SHY>mn` becomes `<U+2003>namn` and `Na<SHY>mn` becomes `Namn`
(Unicode whitespace and upper case kept); `"x"`, `<SHY>"x"`, `"x"<BOM>` and `<SHY><BOM>"x"` become `"x"` (not
unquoted); `<BOM>` and ` <SHY>` become empty (ETL-11.09).

### TC-ETL-044: An append matches each label with the stored name that is cleaned to it with exactly one candidate

Names equal to the labels need no renames; `<BOM>"kopare_id"`, `na<SHY>mn` with the labels `kopare_id`, `namn` are
renamed to them; `<BOM>""x""` (`"x"` with the default parser, `"x""` with the legacy parser) is renamed to whichever
of the two is a label. No result (append fails) if both candidates of a name are labels (`<BOM>""x""` with `"x"` and
`"x""`), if no candidate is a label, if the numbers of names and labels differ (in both directions), or if two names
would get the same label.

### TC-ETL-060: An append keeps stored names that the cleanup does not change and ignores stored names that are empty as a label

1. `zon<U+2003>`, `namn `, `Kommun` with the labels `zon`, `namn`, `kommun`: no renames, the labels are stored under
   the existing names (ETL-11.08)
2. `Na<SHY>mn` with the label `namn` is renamed to `Namn`, the label is stored under `Namn`
3. `<BOM>`, `<BOM>"` and `<U+2003>` (each with `a`, `b`) with the labels `a`, `b`: the name is ignored, no renames
   (ETL-11.09)
4. `namn` and `namn ` with the label `namn`: no result (ambiguous)

### TC-ETL-047: Connection failures, also as causes, stop the column name cleanup, other failures do not

`CannotGetJdbcConnectionException`, `SQLTransientConnectionException`, an `SQLException` with SQL state class 08, and
such an exception as a cause (e.g., in a `CompletionException`) are connection failures; an `SQLException` with
another SQL state, an `UncategorizedSQLException` with such a cause, and other exceptions are not.

### TC-ETL-048: A plain quoted label stored by the default parser is unquoted also if the legacy parser is configured

For the raw first labels `"kopare_id"`, `"a ""b"""`, `"a""b"` and `" x "`, the name stored by the default parser of 1.x
(`<BOM>"kopare_id"`, `<BOM>"a "b""`, `<BOM>"a"b"`, `<BOM>" x "`, verified with the actual parser) is cleaned with the
legacy parser configured to the name a fresh import with the legacy parser yields (`kopare_id`, `a "b"`, `a"b`, `x`).

### TC-ETL-049: An index of a new name keeps its canonical name if an index of an old name had it, by the cleanup and by an append

**Steps:**
1. Verify that `field_<SHY>11246` and `field_100652` have the same first 8 hex digits of their MD5 (`3ef7c3b0`)
2. Create two datasets from `field_11246,field_100652\na,b\nc,d\n`
3. Rename `field_100652` to `field_<SHY>100652` and then `field_11246` to `field_<SHY>11246` in the first dataset (the
   index of `field_<SHY>11246` now has the canonical index name of `field_100652`); invoke the cleanup
4. Break the second dataset the same way; `POST /dataset/{id}` with `field_11246,field_100652\ne,f\n`

**Assertions (both datasets):**
1. The keys are `field_11246`, `field_100652`; the `_jsonidx_` indexes are exactly the canonical ones of these names
   (no `_tmp` index left), and their definitions refer to the right column
2. `?field_100652=d` returns `{field_11246: c, field_100652: d}`
3. Without the temporary index name, the index of `field_100652` was not created (its name existed) and then dropped
   with the index of the old name, leaving the column unindexed

### TC-ETL-061: The cleanup only renames names with a byte order mark or soft hyphens; an append keeps the other stored names

**Steps:**
1. Create a dataset from `kopare_id,namn,ort\n1,Anna,Göteborg\n2,Bo,Mölndal\n`
2. Rename the keys and indexes to `<BOM>"kopare_id"`, `Namn` (upper case as stored before 1.0) and `ort<U+3000>`
   (Unicode whitespace that 1.x did not trim); invoke the cleanup
3. `POST /dataset/{id}` with `kopare_id,namn,ort\n3,Cecilia,Partille\n`

**Assertions:**
1. The cleanup renames the dataset: the keys are `kopare_id`, `Namn`, `ort<U+3000>`, and the `_jsonidx_` indexes are
   the canonical ones of these names; a repeated cleanup does not rename it
2. The append succeeds with 3 rows; the keys and indexes are unchanged, and `?kopare_id=3` returns
   `{kopare_id: 3, Namn: Cecilia, ort<U+3000>: Partille}`

### TC-ETL-062: A stored name that cleans to empty is kept by the cleanup and ignored by an append, and a replace drops it

**Steps:**
1. Create a dataset from `x,a,b\n0,1,2\n1,3,4\n` and rename the keys `x` to `<BOM>` and `a` to `a<SHY>`, as stored
   by 1.x for the index column of pandas (`to_csv(encoding="utf-8-sig")`); invoke the cleanup
2. `POST /dataset/{id}` with `<BOM>,a,b\n2,5,6\n`
3. `PUT /dataset/{id}` with the same file

**Assertions:**
1. The cleanup renames `a<SHY>` to `a` and keeps `<BOM>` with its values
2. The append succeeds with 3 rows; the appended row is `{a: 5, b: 6}`, the previous rows keep `<BOM>`
3. The replace succeeds with 1 row; the keys are `a`, `b`
