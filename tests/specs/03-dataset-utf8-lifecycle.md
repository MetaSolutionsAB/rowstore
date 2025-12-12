# Dataset UTF-8 Lifecycle Specification

## Overview

Tests for creating, querying, managing aliases, and deleting a dataset using UTF-8 comma-separated CSV data.

## Prerequisites

- Running RowStore instance at base URL (default: `http://localhost:8282/`)
- Test data file: `data/dataset1_utf8.csv`

## Test Data

**File:** `dataset1_utf8.csv`
- Encoding: UTF-8
- Delimiter: Comma (`,`)
- Columns: `Name`, `Telephone`, `Some other column`, `Comment`
- Row count: 5 rows

**Content:**
```csv
Name,Telephone,"Some other column","Comment"
Béringer,01234567890,,"No, no comment"
McLoud,0987654321,x,"A comment with five words, and a comma"
Åkesson,,,Another comment with äöå
Martinsson,0,,
Überhuber,555555555,1,2
```

## Test Cases

### TC-DATASET1-001: Create dataset from UTF-8 CSV

**Request:**
- Method: `POST`
- URL: `{baseUrl}/datasets`
- Headers:
  - `Content-Type: text/csv`
- Body: Contents of `dataset1_utf8.csv`

**Expected Response:**
- Status Code: `202 Accepted`
- Content-Type: `application/json`
- Body structure:
  ```json
  {
    "id": "<uuid-string>",
    "url": "<dataset-url>",
    "info": "<info-url>",
    "status": <number>
  }
  ```

**Assertions:**
1. Response status is 202
2. Content-Type header contains `application/json`
3. Response contains `id` (String, UUID format)
4. Response contains `url` (String, URL to dataset)
5. Response contains `info` (String, URL to info endpoint)
6. Response contains `status` (Number)

**Notes:**
- The dataset ID returned is used in subsequent tests
- Dataset processing is asynchronous; status will change from 1 (ACCEPTED_DATA) to 3 (AVAILABLE)

---

### TC-DATASET1-002: Get dataset info

**Depends on:** TC-DATASET1-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/info` (from TC-DATASET1-001 response)
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Content-Type: `application/json`
- Body structure:
  ```json
  {
    "rowcount": 5,
    "created": "<iso-timestamp>",
    "columnnames": ["name", "telephone", "some other column", "comment"],
    "status": 3,
    "@id": "<uri>",
    "@context": "<context-uri>",
    "aliases": [],
    "identifier": "<uuid>"
  }
  ```

**Assertions:**
1. Response status is 200
2. Content-Type header contains `application/json`
3. `rowcount` equals 5
4. `status` equals 3 (AVAILABLE)
5. `columnnames` is an Array
6. `created` is a String (ISO timestamp)
7. `aliases` is an Array (initially empty)
8. `identifier` is a String
9. `@id` is a String
10. `@context` is a String

**Retry Logic:**
- Initial delay: 5000ms (wait for ETL processing)
- Retry count: 2
- Retry delay: 2500ms

---

### TC-DATASET1-003: Query with exact match (Unicode characters)

**Depends on:** TC-DATASET1-001, TC-DATASET1-002

**Request:**
- Method: `GET`
- URL: `{datasetUrl}?Name=Åkesson` (URL encoded: `?Name=%C3%85kesson`)
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Content-Type: `application/json`
- Body:
  ```json
  {
    "results": [
      {
        "name": "Åkesson",
        "telephone": "",
        "some other column": "",
        "comment": "Another comment with äöå"
      }
    ],
    "limit": <number>,
    "offset": <number>,
    "resultCount": <number>
  }
  ```

**Assertions:**
1. Response status is 200
2. `results` array length is 1
3. First result `name` equals "Åkesson"
4. First result `comment` equals "Another comment with äöå"
5. Result contains all expected columns: `name`, `telephone`, `some other column`, `comment`

---

### TC-DATASET1-004: Query with case-insensitive key

**Depends on:** TC-DATASET1-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}?Name=Åkesson` (using "Name" instead of "name")
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Results array length: 1

**Assertions:**
1. Query keys are treated case-insensitively
2. Same results as TC-DATASET1-003

---

### TC-DATASET1-005: Query with exact match (column with spaces)

**Depends on:** TC-DATASET1-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}?Some+other+column=x`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Body:
  ```json
  {
    "results": [
      {
        "name": "McLoud",
        "telephone": "0987654321",
        "some other column": "x",
        "comment": "A comment with five words, and a comma"
      }
    ]
  }
  ```

**Assertions:**
1. Response status is 200
2. `results` array length is 1 (implied)
3. First result matches expected values

---

### TC-DATASET1-006: Query with regular expression

**Depends on:** TC-DATASET1-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}?Name=(Å|é)` (URL encoded: `?Name=(%C3%85%7C%C3%A9)`)
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Results array length: 2

**Assertions:**
1. Response status is 200
2. `results` array length is 2
3. Results contain entries for "Åkesson" and "Béringer"

**Notes:**
- Requires RowStore to be configured with `regexpqueries: "full"` or `regexpqueries: "simple"`

---

### TC-DATASET1-007: Query with non-existing column key

**Depends on:** TC-DATASET1-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}?nonexistingkey=test`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `400 Bad Request`

**Assertions:**
1. Response status is 400
2. Query parameters that don't match column names are rejected

---

### TC-DATASET1-008: Pagination - first page

**Depends on:** TC-DATASET1-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}?Name=(Å|é)&_limit=1`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Body:
  ```json
  {
    "results": [{"name": "Béringer", ...}],
    "offset": 0,
    "limit": 1,
    "resultCount": 2
  }
  ```

**Assertions:**
1. Response status is 200
2. `offset` equals 0
3. `limit` equals 1
4. `resultCount` equals 2
5. `results` array length is 1
6. First result `name` equals "Béringer"

---

### TC-DATASET1-009: Pagination - second page

**Depends on:** TC-DATASET1-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}?Name=(Å|é)&_limit=1&_offset=1`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Body:
  ```json
  {
    "results": [{"name": "Åkesson", ...}],
    "offset": 1,
    "limit": 1,
    "resultCount": 2
  }
  ```

**Assertions:**
1. Response status is 200
2. `offset` equals 1
3. `limit` equals 1
4. `resultCount` equals 2
5. `results` array length is 1
6. First result `name` equals "Åkesson"

---

## Alias Management Tests

### TC-DATASET1-010: Get aliases (initially empty)

**Depends on:** TC-DATASET1-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/aliases`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Content-Type: `application/json`
- Body: `[]`

**Assertions:**
1. Response status is 200
2. Response is an empty JSON array (length 0)

---

### TC-DATASET1-011: Set aliases with PUT

**Depends on:** TC-DATASET1-010

**Request:**
- Method: `PUT`
- URL: `{datasetUrl}/aliases`
- Headers:
  - `Content-Type: application/json`
- Body: `["dataset1"]`

**Expected Response:**
- Status Code: `204 No Content`

**Assertions:**
1. Response status is 204

---

### TC-DATASET1-012: Verify alias was set

**Depends on:** TC-DATASET1-011

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/aliases`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Body: Array with 1 element

**Assertions:**
1. Response status is 200
2. Array length is 1
3. Array contains "dataset1"

---

### TC-DATASET1-013: Access dataset info via alias

**Depends on:** TC-DATASET1-011

**Request:**
- Method: `GET`
- URL: `{baseUrl}/dataset/dataset1/info`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Content-Type: `application/json`
- Body structure same as TC-DATASET1-002

**Assertions:**
1. Response status is 200
2. Response contains expected info structure
3. Alias resolves to the correct dataset

---

### TC-DATASET1-014: Add alias with POST

**Depends on:** TC-DATASET1-012

**Request:**
- Method: `POST`
- URL: `{datasetUrl}/aliases`
- Headers:
  - `Content-Type: application/json`
- Body: `["dataset1b", "dataset1b"]` (intentionally duplicate)

**Expected Response:**
- Status Code: `204 No Content`

**Assertions:**
1. Response status is 204
2. Duplicate aliases in request are handled (only one is added)

---

### TC-DATASET1-015: Verify aliases after POST

**Depends on:** TC-DATASET1-014

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/aliases`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Body: Array with 2 elements

**Assertions:**
1. Response status is 200
2. Array length is 2 (not 3, due to duplicate handling)

---

### TC-DATASET1-016: Delete all aliases

**Depends on:** TC-DATASET1-015

**Request:**
- Method: `DELETE`
- URL: `{datasetUrl}/aliases`

**Expected Response:**
- Status Code: `204 No Content`

**Assertions:**
1. Response status is 204

---

### TC-DATASET1-017: Verify aliases after DELETE

**Depends on:** TC-DATASET1-016

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/aliases`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Body: `[]`

**Assertions:**
1. Response status is 200
2. Array length is 0

---

### TC-DATASET1-018: Alias uniqueness - setup

**Depends on:** TC-DATASET1-017

**Request:**
- Method: `PUT`
- URL: `{datasetUrl}/aliases`
- Headers:
  - `Content-Type: application/json`
- Body: `["theone"]`

**Expected Response:**
- Status Code: `204 No Content`

---

### TC-DATASET1-019: Create second dataset for uniqueness test

**Depends on:** TC-DATASET1-018

**Request:**
- Method: `POST`
- URL: `{baseUrl}/datasets`
- Headers:
  - `Content-Type: text/csv`
- Body: Contents of `dataset1_utf8.csv`

**Expected Response:**
- Status Code: `202 Accepted`

**Notes:**
- Creates dataset1-b for testing alias uniqueness

---

### TC-DATASET1-020: Alias uniqueness - attempt duplicate

**Depends on:** TC-DATASET1-019

**Request:**
- Method: `PUT`
- URL: `{dataset1bUrl}/aliases`
- Headers:
  - `Content-Type: application/json`
- Body: `["theone"]`

**Expected Response:**
- Status Code: `400 Bad Request`

**Assertions:**
1. Response status is 400
2. Cannot assign an alias that is already in use by another dataset

---

### TC-DATASET1-021: Delete second dataset (cleanup)

**Depends on:** TC-DATASET1-020

**Request:**
- Method: `DELETE`
- URL: `{dataset1bUrl}`

**Expected Response:**
- Status Code: `204 No Content`

**Assertions:**
1. Response status is 204

**Retry Logic:**
- Initial delay: 10000ms
- Retry count: 2
- Retry delay: 5000ms
