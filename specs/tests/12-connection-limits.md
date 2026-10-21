# Connection Limit Tests

## Overview

Tests for the connection budgets that limit the number of open database connections per instance: FIFO waiting,
wait timeouts resulting in 503, release of connections, independent budgets, configuration, usage reporting, and
requests and ETL jobs holding at most one connection at a time. See
[01-system-architecture.md](../01-system-architecture.md) (ARCH-7) and [06-configuration.md](../06-configuration.md)
(CFG-4.05 to CFG-4.07).

TC-CONN-001 to TC-CONN-007 are unit tests of the budget (`LimitedDataSourceTest`) with a fake data source,
TC-CONN-008 and TC-CONN-009 unit tests of the configuration (`RowStoreConfigConnectionLimitsTest`). TC-CONN-010 to
TC-CONN-013 are integration tests with small limits (`ConnectionLimitIT`: 2 query connections, 1 export connection,
`maxetlprocesses` 2, wait timeout 2 s), TC-CONN-014 to TC-CONN-018 integration tests with one connection per budget
(`ConnectionLimitSingleSlotIT`: 1 query connection, 1 export connection, `maxetlprocesses` 1, wait timeout 5 s).

## Prerequisites

- Unit tests: none
- Integration tests: a RowStore instance started with the limits above and direct database access (to lock data
  tables and to change stored column names)

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

### TC-CONN-007: Statements and result sets return the limited connection, closing it releases the slot

**Assertions:**
1. `Statement.getConnection()` returns the limited connection and `ResultSet.getStatement()` the limited statement
2. Closing the connection obtained via `rs.getStatement().getConnection()` (as the export does) releases the slot

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
   `rowstore.json: '<block>.<key>' must be greater than 0`

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

### TC-CONN-012: A request gets 503 if all query connections stay in use for longer than the wait timeout

**Steps:**
1. Lock the data table of dataset A exclusively with a direct database connection
2. Send two queries of dataset A; wait until both wait for the lock (each holding a query connection)
3. `GET /dataset/{B}/info`
4. Release the lock

**Assertions:**
1. Step 3 returns 503 after at least the wait timeout (2 s)
2. After the lock is released, both queries of step 2 return 200, and step 3 returns 200 again

### TC-CONN-013: A long export does not block queries, an excess export fails after the wait timeout

**Steps:**
1. Lock the data table of dataset A exclusively with a direct database connection
2. Export dataset A as JSON; wait until it waits for the lock (holding the only export connection)
3. Query dataset B, then export dataset B as JSON
4. Release the lock

**Assertions:**
1. The query of step 3 returns 200 with 1 result within less than the wait timeout
2. The export of step 3 takes at least the wait timeout and fails: 503 if the export connection is requested before
   the response status is sent, otherwise a truncated response (API-6.05)
3. After the lock is released, the export of step 2 returns all 5 rows, a further export of B returns 200, and no
   export connection is in use

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
