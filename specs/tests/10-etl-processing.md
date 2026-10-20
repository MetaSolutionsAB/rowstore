# ETL Processing Tests

## Overview

Tests for the failure handling of ETL loads: status handling when a load fails or the database is unreachable.
See [04-etl-pipeline.md](../04-etl-pipeline.md) (ETL-10).

TC-ETL-007 to TC-ETL-009 are unit tests (`PgDatasetPopulateTest`) that use a fake database access layer.

## Prerequisites

None

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
