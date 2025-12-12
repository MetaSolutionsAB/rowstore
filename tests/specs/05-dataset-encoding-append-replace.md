# Dataset Encoding, Append, and Replace Specification

## Overview

Tests for creating a dataset from Windows-1252 encoded CSV, appending data to an existing dataset, and replacing dataset content.

## Prerequisites

- Running RowStore instance at base URL (default: `http://localhost:8282/`)
- Test data files:
  - `data/dataset3_windows1252.csv`
  - `data/dataset2_utf8_semicolon.csv`

## Test Data

**File:** `dataset3_windows1252.csv`
- Encoding: Windows-1252 (CP1252)
- Delimiter: Comma (`,`)
- Columns: `Name`, `Telephone`, `Some other column`, `Comment`
- Row count: 5 rows

**Content (represented as UTF-8):**
```csv
Name,Telephone,"Some other column","Comment"
Béringer,01234567890,,"No, no comment"
McLoud,0987654321,x,"A comment with five words, and a comma"
Åkesson,,,Another comment with äöå
Martinsson,0,,
Überhuber,555555555,1,2
```

**Note:** The actual file uses Windows-1252 encoding where characters like `é`, `Å`, `ä`, `ö`, `å`, `Ü` are encoded differently than in UTF-8.

## Test Cases

### TC-DATASET3-001: Create dataset from Windows-1252 CSV

**Request:**
- Method: `POST`
- URL: `{baseUrl}/datasets`
- Headers:
  - `Content-Type: text/csv`
- Body: Contents of `dataset3_windows1252.csv` (binary, Windows-1252 encoded)

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
2. Response contains dataset identifiers

---

### TC-DATASET3-002: Get dataset info

**Depends on:** TC-DATASET3-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/info`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Body:
  ```json
  {
    "rowcount": 5,
    "status": 3,
    "columnnames": [...],
    ...
  }
  ```

**Assertions:**
1. Response status is 200
2. `rowcount` equals 5
3. `status` equals 3 (AVAILABLE)
4. `columnnames` is an Array

**Retry Logic:**
- Initial delay: 5000ms
- Retry count: 2
- Retry delay: 2500ms

---

### TC-DATASET3-003: Query with exact match (verify encoding conversion)

**Depends on:** TC-DATASET3-002

**Request:**
- Method: `GET`
- URL: `{datasetUrl}?Name=Åkesson` (URL encoded: `?Name=%C3%85kesson`)
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Body:
  ```json
  {
    "results": [
      {
        "name": "Åkesson",
        "comment": "Another comment with äöå"
      }
    ]
  }
  ```

**Assertions:**
1. Response status is 200
2. First result `name` equals "Åkesson" (properly converted from Windows-1252)
3. First result `comment` equals "Another comment with äöå"
4. Unicode characters are correctly converted from Windows-1252 to UTF-8

---

### TC-DATASET3-004: Append data to existing dataset

**Depends on:** TC-DATASET3-003

**Request:**
- Method: `POST`
- URL: `{datasetUrl}` (from TC-DATASET3-001)
- Headers:
  - `Content-Type: text/csv`
- Body: Contents of `dataset2_utf8_semicolon.csv`

**Expected Response:**
- Status Code: `202 Accepted`
- Content-Type: `application/json`

**Assertions:**
1. Response status is 202
2. Existing dataset URL accepts POST for appending

**Notes:**
- POST to an existing dataset URL appends data
- No structural integrity check is performed (different columns could be added)

---

### TC-DATASET3-005: Verify row count after append

**Depends on:** TC-DATASET3-004

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/info`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Body:
  ```json
  {
    "rowcount": 10,
    "status": 3,
    ...
  }
  ```

**Assertions:**
1. Response status is 200
2. `rowcount` equals 10 (5 original + 5 appended)
3. `status` equals 3 (AVAILABLE)

**Retry Logic:**
- Initial delay: 5000ms
- Retry count: 2
- Retry delay: 2500ms

---

### TC-DATASET3-006: Replace dataset content

**Depends on:** TC-DATASET3-005

**Request:**
- Method: `PUT`
- URL: `{datasetUrl}` (from TC-DATASET3-001)
- Headers:
  - `Content-Type: text/csv`
- Body: Contents of `dataset3_windows1252.csv`

**Expected Response:**
- Status Code: `202 Accepted`
- Content-Type: `application/json`

**Assertions:**
1. Response status is 202
2. PUT replaces existing dataset content

---

### TC-DATASET3-007: Verify row count after replace

**Depends on:** TC-DATASET3-006

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/info`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Body:
  ```json
  {
    "rowcount": 5,
    "status": 3,
    ...
  }
  ```

**Assertions:**
1. Response status is 200
2. `rowcount` equals 5 (back to original count)
3. `status` equals 3 (AVAILABLE)

**Retry Logic:**
- Initial delay: 5000ms
- Retry count: 2
- Retry delay: 2500ms

## Summary

This test sequence verifies:
1. Windows-1252 encoding is correctly detected and converted to UTF-8
2. POST to existing dataset URL appends data
3. PUT to existing dataset URL replaces all data
4. Row counts reflect append and replace operations correctly
