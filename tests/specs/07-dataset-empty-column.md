# Dataset Empty Column Label Specification

## Overview

Tests for handling CSV files with empty column labels. Verifies that RowStore handles columns without names gracefully.

## Prerequisites

- Running RowStore instance at base URL (default: `http://localhost:8282/`)
- Test data file: `data/dataset5_utf8_emptycolumn.csv`

## Test Data

**File:** `dataset5_utf8_emptycolumn.csv`
- Encoding: UTF-8
- Delimiter: Comma (`,`)
- Issue: Header row has trailing empty column labels

**Content:**
```csv
Name,Telephone,"Some other column","Comment",,
Béringer,01234567890,,"No, no comment"
McLoud,0987654321,x,"A comment with five words, and a comma"
Åkesson,,,Another comment with äöå
Martinsson,0,,
Überhuber,555555555,1,2
```

**Note:** The header row ends with `,,` indicating two empty column labels after "Comment".

## Test Cases

### TC-DATASET5-001: Create dataset with empty column labels

**Request:**
- Method: `POST`
- URL: `{baseUrl}/datasets`
- Headers:
  - `Content-Type: text/csv`
- Body: Contents of `dataset5_utf8_emptycolumn.csv`

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

---

### TC-DATASET5-002: Verify column count

**Depends on:** TC-DATASET5-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/info` (from TC-DATASET5-001 response)
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Content-Type: `application/json`
- Body contains:
  ```json
  {
    "columnnames": ["name", "telephone", "some other column", "comment"],
    ...
  }
  ```

**Assertions:**
1. Response status is 200
2. `columnnames` array length is 4

**Retry Logic:**
- Initial delay: 5000ms
- Retry count: 2
- Retry delay: 2500ms

## Summary

This test verifies that:
1. CSV files with empty column labels are processed successfully
2. Empty column labels are excluded from the column names list
3. Only columns with actual labels are included in `columnnames` (4 named columns)
