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
3. Response body contains `service` field of type String
4. Response body contains `datasets` field of type Number
5. Response body contains `activeEtlProcesses` field of type Number
6. Response body contains `version` field of type String
