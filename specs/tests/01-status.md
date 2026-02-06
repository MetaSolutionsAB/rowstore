# Status Endpoint Specification

## Overview

Tests for the `/status` endpoint which returns information about the RowStore instance.

## Prerequisites

- Running RowStore instance at base URL (default: `http://localhost:8282/`)

## Test Cases

### TC-STATUS-001: Get status and check structure

**Request:**
- Method: `GET`
- URL: `{baseUrl}/status`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Content-Type: `application/json`
- Body structure (JSON):
  ```json
  {
    "service": "<string>",
    "datasets": <number>,
    "activeEtlProcesses": <number>,
    "version": "<string>"
  }
  ```

**Assertions:**
1. Response status is 200
2. Content-Type header contains `application/json`
3. Response body contains `service` field of type String equal to "RowStore"
4. Response body contains `datasets` field of type Number >= 0
5. Response body contains `activeEtlProcesses` field of type Number >= 0
6. Response body contains `version` field of type String

---

### TC-STATUS-002: Get status with JVM parameter

**Request:**
- Method: `GET`
- URL: `{baseUrl}/status?jvm`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Content-Type: `application/json`
- Body structure (JSON):
  ```json
  {
    "service": "<string>",
    "datasets": <number>,
    "activeEtlProcesses": <number>,
    "version": "<string>",
    "jvm": {
      "totalMemory": <number>,
      "freeMemory": <number>,
      "maxMemory": <number>,
      "availableProcessors": <number>
    }
  }
  ```

**Assertions:**
1. Response status is 200
2. Content-Type header contains `application/json`
3. Response body contains `jvm` object
4. `jvm.totalMemory` is present and is a positive number
5. `jvm.freeMemory` is present and is a positive number
6. `jvm.maxMemory` is present and is a positive number
7. `jvm.availableProcessors` is present and is a positive integer

---

### TC-STATUS-003: JVM memory values are valid

**Request:**
- Method: `GET`
- URL: `{baseUrl}/status?jvm`
- Headers:
  - `Accept: application/json`

**Assertions:**
1. `jvm.freeMemory` <= `jvm.totalMemory` (free cannot exceed total)
2. `jvm.totalMemory` <= `jvm.maxMemory` (total cannot exceed max)
3. `jvm.availableProcessors` >= 1 (at least one processor)

---

### TC-STATUS-004: Status datasets count tracks dataset creation

**Precondition:**
1. Record initial datasets count from `/status`

**Steps:**
1. Create a new dataset via `POST /datasets`
2. Wait for dataset to be available
3. Query `/status` again

**Assertions:**
1. New datasets count >= initial count (dataset created)
2. After cleanup, count returns to initial or decrements

---

### TC-STATUS-005: Status activeEtlProcesses tracking

**Description:**
During ETL processing, `activeEtlProcesses` should be > 0. After completion, it can be 0.

**Steps:**
1. Create a dataset (POST returns 202)
2. Immediately query `/status`
3. If status queried during processing, activeEtlProcesses may be > 0
4. After dataset is available, activeEtlProcesses can be >= 0

**Assertions:**
1. `activeEtlProcesses` is always >= 0 (never negative)
