# Configuration Tests

## Overview

Tests for parsing and validating `rowstore.json` (see [CFG-1.02](../06-configuration.md#cfg-1-file-format)) and for how database settings are applied to the data sources ([CFG-4](../06-configuration.md#cfg-4-database-configuration), [CFG-5](../06-configuration.md#cfg-5-read-replica-configuration)).

- TC-CFG-* and TC-DS-* are unit tests (`RowStoreConfigTest`, `DataSourceConfigTest`); they need neither Docker nor a running server.
- TC-POOL-* are integration tests (`ConnectionPoolIT`) against an in-process server with `connectionPoolMax=3`, `ssl=false`, `socketTimeout=60` and a separate `queryDatabase` pointing to the same container. TC-POOL-006 to TC-POOL-008 inspect the DataSource beans and are skipped against an external instance. TC-POOL-001 to TC-POOL-005 (status, lifecycle, query and HikariCP metrics with a pool) are defined in the test class.

## Test Cases

### TC-CFG-001: Valid full config is exposed by all getters

**Input:** Config with every supported key set to a non-default value, including `queryDatabase`, `ratelimit` and `cors`.

**Assertions:**
1. Every getter returns the configured value
2. `getQueryDatabase()` is a separate object with the `queryDatabase` values

### TC-CFG-002: Minimal config applies documented defaults

**Input:** Only `baseurl` and `database` with `host`, `database`, `user`, `password`.

**Assertions:**
1. `querymaxlimit` = 100, `maxuploadsize` = 104857600, `querytimeout` = -1, `cors.allowedorigins` = `["*"]`
2. Database defaults: `type` = postgresql, `port` = 5432, `ssl` = false, `socketTimeout` = 0, `connectTimeout` = 5, `loginTimeout` = 5, pool settings = -1
3. `getQueryDatabase()` is the same object as `getDatabase()`

### TC-CFG-003: Empty database password is accepted

**Input:** Minimal config with `"password": ""`.

**Assertions:**
1. Construction succeeds and the password is empty

### TC-CFG-004: Missing required key fails and names the key

**Input (one per case):** Full config without `baseurl`, `database`, `database.host`, `database.database`, `database.user`, `database.password`, `queryDatabase.host`, `queryDatabase.database`, `queryDatabase.user` or `queryDatabase.password`.

**Assertions:**
1. `IllegalStateException` is thrown
2. Message contains `missing required key '<key>'`

### TC-CFG-005: Key with wrong type fails instead of silently using defaults

**Input (one per case):** `"ratelimit": "yes"`, `"cors": "x"`, `"cors": {"allowedorigins": "x"}`, `"cors": {"allowedorigins": [1]}`, `"database": "localhost"`, `"queryDatabase": true`, `"baseurl": 42`.

**Assertions:**
1. `IllegalStateException` is thrown
2. Message starts with `rowstore.json` and names the key

### TC-CFG-006: Non-positive querymaxlimit or maxuploadsize fails

**Input (one per case):** `querymaxlimit` = 0 or -1, `maxuploadsize` = 0 or -5.

**Assertions:**
1. `IllegalStateException` is thrown and the message names the key

### TC-CFG-007: Unknown and retired keys are ignored

**Input:** Minimal config plus `exportpagesize` (retired, CFG-3.08) and an unknown key.

**Assertions:**
1. Construction succeeds

### TC-CFG-008: trustedproxies default, explicit list and empty array

**Input (one per case):** Minimal config without `trustedproxies`; with `["192.0.2.10", "10.10.0.0/16", "2001:db8::/32", "::1"]`; with `[]`.

**Assertions:**
1. Without the key, the default list of CFG-3.11 applies (loopback, private, shared and link-local ranges)
2. An explicit list is used as configured
3. An empty array results in an empty list (nobody is trusted)

### TC-CFG-009: Invalid trustedproxies fails and names the key

**Input (one per case):** `trustedproxies` = a string, an object, `[1]`, an array containing `null`, or an array with an entry that is not an IP literal with optional prefix length: `"not-an-ip"`, `"localhost"`, `"10.0.0"`, `"10.0.0.256"`, `"10.0.0.0/33"`, `"10.0.0.0/-1"`, `"10.0.0.0/abc"`, `"10.0.0.0/8;80"`, `"::1/129"`, `"fe80::1%lo"` (zone ID), `""`.

**Assertions:**
1. Construction fails with `IllegalStateException`
2. The message starts with `rowstore.json` and names `'trustedproxies'`

### TC-DS-001: Unpooled query data source applies SSL mode, timeouts and read-only mode

**Input (one per case):** Config with a separate `queryDatabase` without pool, `ssl` = false or true, `socketTimeout` = 60, `connectTimeout` = 7, `loginTimeout` = 8.

**Assertions:**
1. `sslmode` is `disable` for `ssl=false` and `require` for `ssl=true`
2. Driver timeouts are 60 / 7 / 8 seconds
3. `readOnly` is true and `readOnlyMode` is `always`

### TC-POOL-006: SSL mode and driver timeouts are passed to the HikariCP pools

**Assertions (primary and query pool):**
1. Data source properties contain `sslmode=disable`, `connectTimeout=5`, `loginTimeout=5`, `socketTimeout=60`

### TC-POOL-007: Query pool is read-only, also in auto-commit mode

**Assertions:**
1. The query pool is a separate pool with data source property `readOnlyMode=always`; the primary pool has no `readOnlyMode`
2. `SHOW transaction_read_only` returns `on` on a query pool connection and `off` on a primary pool connection

### TC-POOL-008: Connections do not use SSL when ssl is false

**Assertions (primary and query pool):**
1. `SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()` returns false

**Note:** The Testcontainers PostgreSQL image runs without SSL, so this cannot distinguish `sslmode=disable` from `prefer`; TC-POOL-006 asserts the configured mode.
