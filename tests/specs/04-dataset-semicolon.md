# Dataset Semicolon-Separated CSV Specification

## Overview

Tests for creating and querying a dataset using UTF-8 semicolon-separated CSV data. This tests RowStore's ability to detect and handle alternative CSV delimiters.

## Prerequisites

- Running RowStore instance at base URL (default: `http://localhost:8282/`)
- Test data file: `data/dataset2_utf8_semicolon.csv`

## Test Data

**File:** `dataset2_utf8_semicolon.csv`
- Encoding: UTF-8
- Delimiter: Semicolon (`;`)
- Columns: `Name`, `Telephone`, `Some other column`, `Comment`
- Row count: 5 rows

**Content:**
```csv
Name;Telephone;"Some other column";"Comment"
Béringer;01234567890;;No, no comment
McLoud;0987654321;x;A comment with five words, and a comma
Åkesson;;;Another comment with äöå
Martinsson;0;;
Überhuber;555555555;1;2
```

## Test Cases

### TC-DATASET2-001: Create dataset from semicolon-separated CSV

**Request:**
- Method: `POST`
- URL: `{baseUrl}/datasets`
- Headers:
  - `Content-Type: text/csv`
- Body: Contents of `dataset2_utf8_semicolon.csv`

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
4. Response contains `url` (String)
5. Response contains `info` (String)
6. Response contains `status` (Number)

---

### TC-DATASET2-002: Get dataset info

**Depends on:** TC-DATASET2-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/info` (from TC-DATASET2-001 response)
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Content-Type: `application/json`
- Body:
  ```json
  {
    "rowcount": 5,
    "status": 3,
    "columnnames": [...],
    "created": "<timestamp>",
    ...
  }
  ```

**Assertions:**
1. Response status is 200
2. Content-Type header contains `application/json`
3. `rowcount` equals 5
4. `status` equals 3 (AVAILABLE)
5. `columnnames` is an Array

**Retry Logic:**
- Initial delay: 5000ms
- Retry count: 2
- Retry delay: 2500ms

---

### TC-DATASET2-003: Query with exact match

**Depends on:** TC-DATASET2-002

**Request:**
- Method: `GET`
- URL: `{datasetUrl}?Name=Béringer` (URL encoded: `?Name=B%C3%A9ringer`)
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
        "name": "Béringer",
        "comment": "No, no comment"
      }
    ]
  }
  ```

**Assertions:**
1. Response status is 200
2. `results` array contains expected data
3. First result `name` equals "Béringer"
4. First result `comment` equals "No, no comment"

**Notes:**
- This confirms that semicolon-separated CSV was correctly parsed
- Column names and values should match expected structure
