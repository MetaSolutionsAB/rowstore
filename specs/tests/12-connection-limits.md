# Connection Limit Tests

## Overview

Tests for the connection budgets that limit the number of open database connections per instance: FIFO waiting,
wait timeouts resulting in 503, release of connections, independent budgets, configuration, usage reporting, and
requests and ETL jobs holding at most one connection at a time. See
[01-system-architecture.md](../01-system-architecture.md) (ARCH-7) and [06-configuration.md](../06-configuration.md)
(CFG-4.05 to CFG-4.08).

TC-CONN-001 to TC-CONN-006 and TC-CONN-024 to TC-CONN-026 are unit tests of the budget (`LimitedDataSourceTest`)
with a fake data source, TC-CONN-008, TC-CONN-009 and TC-CONN-028 unit tests of the configuration
(`RowStoreConfigConnectionLimitsTest`), TC-CONN-027 a unit test of the session statements (`PgRowStoreSessionSqlTest`).
TC-CONN-010 to TC-CONN-013, TC-CONN-021 and TC-CONN-029 are integration tests with small
limits (`ConnectionLimitIT`: 2 query connections, 1 export connection, `maxetlprocesses` 2, wait timeout 2 s, hence
lock timeout 1 s), TC-CONN-014 to TC-CONN-020 and TC-CONN-022 integration tests with one connection per budget
(`ConnectionLimitSingleSlotIT`: 1 query connection, 1 export connection, `maxetlprocesses` 1, wait timeout 5 s, hence
lock timeout 2.5 s), TC-CONN-023 an integration test with an unreachable separate query database
(`ConnectionLimitHealthIT`), TC-CONN-030 to TC-CONN-033 unit tests of the retries at startup (`StartupRetryTest`),
TC-CONN-034 and TC-CONN-035 unit tests of the 503 responses (`DatabaseExceptionHandlerTest`), and TC-CONN-036 an
integration test of the startup with an unreachable database (`DatabaseUnavailableAtStartupIT`, with its own
PostgreSQL container).

Some tests make requests hold their connections by making the queries of a dataset slow: its data table is renamed and
replaced by a view that calls `pg_sleep` ("slow dataset"); afterwards the sleeping queries are cancelled and the data
table is restored.

## Prerequisites

- Unit tests: none
- Integration tests: a RowStore instance started with the limits above and direct database access (to lock data
  tables, to change stored column names, to make datasets slow and to read stored statuses)

## Test Data

`dataset1_utf8.csv` (5 rows, columns `name`, `telephone`, `some other column`, `comment`).

## Budget (unit tests)

### TC-CONN-001: Waiting requests get a connection in FIFO order

**Steps:**
1. Budget with 1 connection. Hold the connection
2. Start five requests one after another, each after the previous one is waiting
3. Close the held connection

**Assertions:**
1. The requests get the connection in the order they started
2. Afterwards no connection is in use and no request is waiting

### TC-CONN-002: A request waits for a free connection and gets it when one is closed

**Assertions:**
1. While the only connection is held, a request waits (usage: 1 in use, 1 waiting) and does not complete
2. When the held connection is closed, the request gets a connection

### TC-CONN-003: A wait timeout throws an exception that maps to 503, also via JdbcTemplate

**Steps:**
1. Budget with 1 connection and a wait timeout of 300 ms. Hold the connection
2. Request a connection directly and via `JdbcTemplate`

**Assertions:**
1. The direct request fails after at least 300 ms with `SQLTransientConnectionException`
2. `JdbcTemplate` throws `CannotGetJdbcConnectionException` (a `DataAccessResourceFailureException`), which
   `DatabaseExceptionHandler` maps to 503
3. No connection of the target data source is opened for the timed out requests

### TC-CONN-004: Closing a connection releases its slot exactly once, also on double close and failing close

**Assertions:**
1. Closing a connection twice, or a connection whose close fails, leaves no connection in use
2. Afterwards exactly as many connections as the limit can be open at the same time

### TC-CONN-005: A failure to open a connection releases the slot

**Assertions:**
1. If the target data source fails to open a connection, the failure propagates and no connection is in use

### TC-CONN-006: Budgets on the same data source are independent

**Assertions:**
1. While an export budget with 1 connection is exhausted, both connections of a query budget can be opened without
   waiting

### TC-CONN-007: [REMOVED] Statements and result sets return the limited connection, closing it releases the slot

Only the connection is limited since the export closes the connection it obtained (RowStream) instead of the
connection of the statement of its result set.

## Configuration (unit tests)

### TC-CONN-008: Connection limits default to 10 query and 2 export connections and a wait timeout of 30 s

**Assertions:**
1. Without settings: `maxQueryConnections` 10, `maxExportConnections` 2, `connectionWaitTimeout` 30
2. Settings are read per database block; without `connectionWaitTimeout`, the wait timeout is `querytimeout` if
   that is greater than 0

### TC-CONN-009: Connection limits that are not greater than 0 are rejected at startup

**Assertions:**
1. `0`, negative values and non-numeric values of `maxQueryConnections`, `maxExportConnections` and
   `connectionWaitTimeout` in `database` or `queryDatabase` are rejected with
   `rowstore.json: '<block>.<key>' must be a number greater than 0`

## Small limits (integration tests)

### TC-CONN-010: Status reports the usage of the connection budgets

**Request:** `GET /status`

**Assertions:**
1. Status 200
2. `connections.database.query.max` is 2, `connections.database.export.max` is 1, `connections.database.load.max`
   is 2; `inUse` and `waiting` are numbers
3. There is no `connections.queryDatabase`

### TC-CONN-011: A burst of more concurrent queries than query connections succeeds without 503

**Steps:**
1. Send 16 concurrent queries `GET /dataset/{id}?name=Åkesson`

**Assertions:**
1. All queries return 200

### TC-CONN-012: Requests get 503 if all query connections stay in use for longer than the wait timeout, status is still reported

**Steps:**
1. Make dataset A slow (30 s)
2. Send two queries of dataset A; wait until both sleep (each holding a query connection)
3. `GET /dataset/{B}/info`, `GET /datasets`, `GET /status`
4. Cancel the sleeping queries and restore dataset A

**Assertions:**
1. `GET /dataset/{B}/info` returns 503 after at least the wait timeout (2 s)
2. `GET /datasets` returns 503
3. `GET /status` returns 200 with `datasets` -1 and `connections.database.query.inUse` 2
4. The cancelled queries return 503; afterwards the info of A and B returns 200

### TC-CONN-013: A long export does not block queries, an excess export fails after the wait timeout

**Steps:**
1. Lock the data table of dataset A exclusively with a direct database connection
2. Export dataset A as JSON; wait until it waits for the lock (holding the only export connection)
3. Query dataset B, then export dataset B as JSON
4. Release the lock

**Assertions:**
1. The query of step 3 returns 200 with 1 result within less than the wait timeout
2. The export of step 3 takes at least the wait timeout and fails with 503: the export connection is requested before
   the response status is sent (API-6.05)
3. After the lock is released, the export of step 2 returns all 5 rows, a further export of B returns 200, and no
   export connection is in use

### TC-CONN-021: Requests for a dataset blocked behind a queued exclusive lock get 503 after the lock timeout, other datasets are served

**Steps:**
1. Hold a share lock on the data table of dataset A (a long reader) and queue an exclusive lock behind it (like a
   replace waiting to swap the data table)
2. Send three queries of dataset A (more than query connections); wait until they wait for the lock
3. Query dataset B
4. Release the locks

**Assertions:**
1. The query of dataset B returns 200 with 1 result within the lock timeout (1 s) plus 1 s
2. The queries of dataset A return 503 (lock timeout)
3. After the locks are released, queries of dataset A return 200

## One connection per budget (integration tests)

### TC-CONN-014: Appends and replaces work with one connection per budget

**Assertions:**
1. Creating a dataset, appending the same CSV (`POST /dataset/{id}`) and replacing it (`PUT /dataset/{id}`) lead to
   status AVAILABLE with 5, 10 and 5 rows

### TC-CONN-015: Alias PUT and POST complete within the wait timeout with one connection per budget

**Assertions:**
1. `PUT /dataset/{id}/aliases` with `["singleslotone"]` and `POST` with `["singleslottwo"]` return 204 within 4 s;
   `GET` returns both aliases
2. An alias consisting of the 32 hexadecimal digits of another dataset's ID is rejected with 400 within 4 s
3. An alias of another dataset is rejected with 400 within 4 s

### TC-CONN-016: Column names are cleaned by an append with one connection per budget

**Steps:**
1. Create a dataset and rename its stored column `name` to `<BOM>name` with a direct database connection
2. Append the same CSV

**Assertions:**
1. The append succeeds, and `?name=Åkesson` returns 2 results within 4 s

### TC-CONN-017: JSON and CSV exports work with one connection per budget

**Assertions:**
1. The JSON export returns 5 rows and the CSV export a header and 5 rows, each within 4 s

### TC-CONN-018: Health check and status complete within the wait timeout with one connection per budget

**Assertions:**
1. `GET /actuator/health` returns status UP within 4 s
2. `GET /status` reports `max` 1 for the query, export and load budgets within 4 s

### TC-CONN-019: A load does not hold a query connection while it holds its load connection, for appends and replaces

**Steps (for an append and for a replace):**
1. Lock the data table of a dataset exclusively; submit the load; wait until the load waits for the lock
2. `GET /status`
3. Release the lock

**Assertions:**
1. `connections.database.load.inUse` is 1 and `connections.database.query.inUse` is 0
2. Both loads succeed; the dataset has 5 rows

### TC-CONN-020: A load writes its final status while all query connections are in use

**Steps:**
1. Lock the data table of dataset A exclusively; submit an append; wait until the load waits for the lock
2. Make dataset B slow (30 s) and send a query of B (holding the only query connection)
3. Release the lock

**Assertions:**
1. The stored status of A is AVAILABLE within 4 s (less than the wait timeout), while the query of B still holds the
   query connection
2. A has 10 rows

### TC-CONN-022: Health check and status do not wait for the wait timeout if all query connections are in use

**Steps:**
1. Make a dataset slow (30 s) and send a query of it (holding the only query connection)

**Assertions:**
1. `GET /actuator/health` returns 200 with status UP within 4 s (the database is reported as busy)
2. `GET /status` returns 200 with `datasets` -1 and `connections.database.query.inUse` 1 within 4 s

## Separate query database (integration test)

### TC-CONN-023: An unreachable separate query database is reported as DOWN by the health check

**Steps:**
1. Start RowStore with a `queryDatabase` like `database`, but with a port on which no database listens

**Assertions:**
1. `GET /actuator/health` returns 503 with status DOWN
2. `GET /status` (primary database) returns 200 with a dataset count, budgets `connections.queryDatabase.query` (max
   10) and `connections.queryDatabase.export` (max 2), and no `connections.database.export`

## Budget details (unit tests)

### TC-CONN-024: A wait timeout is distinguished from a failure to open a connection

**Assertions:**
1. A wait timeout is a `ConnectionWaitTimeoutException` (an `SQLTransientConnectionException`), recognized by
   `LimitedDataSource.isWaitTimeout` also as a cause (e.g., in `CannotGetJdbcConnectionException` and
   `CompletionException`), so that, e.g., the column name cleanup continues instead of stopping as if the database
   were unreachable
2. A failure to open a connection and other `SQLTransientConnectionException`s are not wait timeouts

### TC-CONN-025: A data source with a shorter wait timeout shares the slots, the session statement runs on every connection

**Assertions:**
1. While the only connection is held, a data source created with `withWaitTimeout(100)` times out after about 100 ms
   and reports the same usage; it uses the same slot
2. The session statement (e.g., `SET lock_timeout = 5000`) is executed on every connection
3. If the session statement fails, the connection is closed and the slot released

### TC-CONN-026: A limited connection is equal only to itself

**Assertions:**
1. A limited connection is equal to itself and not to another one, with a stable hash code

## Lock timeout configuration (unit tests)

### TC-CONN-027: Session statements of the budgets

**Assertions:**
1. By default (wait timeout 30 s), query connections execute `SET lock_timeout = 15000`; export and load connections
   execute nothing without a pool and `RESET lock_timeout` with a HikariCP pool
2. With `queryLockTimeout` 5, query connections execute `SET lock_timeout = 5000`
3. With `queryLockTimeout` 0, no connection of any budget executes a session statement, with or without a pool

### TC-CONN-028: queryLockTimeout must be a number greater than or equal to 0

**Assertions:**
1. Negative and non-numeric values in `database` or `queryDatabase` are rejected with
   `rowstore.json: '<block>.queryLockTimeout' must be a number greater than or equal to 0`
2. Without a value, the lock timeout is half the wait timeout (wait timeout 9 s: 4500 ms); `0` disables it

## Lock timeout of data changes (integration test)

### TC-CONN-029: Creating a dataset gets 503 if the datasets table stays locked for longer than the lock timeout

**Steps:**
1. Lock the `datasets` table in share mode with a direct database connection
2. `POST /datasets` with `dataset1_utf8.csv`
3. Release the lock

**Assertions:**
1. Step 2 returns 503 after at least the lock timeout (1 s)
2. No dataset was created

## Database not reachable at startup (unit tests)

### TC-CONN-030: Connectivity failures at startup are retried with growing delays until the task succeeds

**Setup:** `StartupRetry` with an initial delay of 1 s, a maximum delay of 60 s and a fake sleep; the task fails eight
times with a `SQLException` with SQL state `08001` (wrapped), then succeeds.

**Assertions:**
1. The task's result is returned after nine attempts
2. The delays are 1, 2, 4, 8, 16, 32, 60 and 60 seconds

### TC-CONN-031: Other failures at startup, e.g., of a migration or the authentication, are not retried

**Assertions:**
1. A runtime exception with a cause with SQL state `42601` (a failing migration) is thrown as it is, after one attempt
2. A checked `SQLException` with SQL state `28P01` (wrong password) is thrown after one attempt, wrapped in an
   `IllegalStateException`; there is no delay

### TC-CONN-032: Connection failures, an unavailable database and failed connection attempts count as connectivity failures

**Assertions:**
1. SQL states `08001`, `57P03`, `57P01` and `53300`, `SQLTransientConnectionException`,
   `CannotGetJdbcConnectionException` and a `ConnectException` cause are connectivity failures
2. SQL states `28P01` and `42P01` and other exceptions are not

### TC-CONN-033: An interruption while waiting for the database fails the startup

**Assertions:**
1. If the sleep between two attempts is interrupted, an `IllegalStateException` is thrown and the thread's interrupt
   flag is set

## 503 responses (unit tests)

### TC-CONN-034: Unavailability of the database results in 503 with Retry-After

**Assertions:**
1. A wait timeout of a budget, an unreachable database and a lock timeout each result in 503 with an empty body and
   `Retry-After: 5`

### TC-CONN-035: A failure after a streamed response was committed is rethrown

**Assertions:**
1. If the response is already committed, the exception is rethrown instead of being mapped to 503

## Database not reachable at startup (integration test)

### TC-CONN-036: RowStore waits at startup for a database that is not reachable and starts once it is

**Steps:**
1. Start a separate PostgreSQL container and pause it, so that connection attempts time out
2. Start RowStore against it (connect and login timeout 2 s) in the background
3. Wait 10 seconds, then unpause the container

**Assertions:**
1. After step 3's wait, the startup has neither completed nor failed, and RowStore does not listen on its port
2. After unpausing, the startup completes within 90 seconds, `GET /status` returns 200, and the migration ran exactly
   once (one successful row of version 1 in `flyway_schema_history`)
