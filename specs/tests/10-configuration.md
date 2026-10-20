# Configuration Tests

## Overview

Tests for parsing and validating `rowstore.json` (see [CFG-1.02](../06-configuration.md#cfg-1-file-format)). These are unit tests (`RowStoreConfigTest`) that construct `RowStoreConfig` directly from JSON; they need neither Docker nor a running server.

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
