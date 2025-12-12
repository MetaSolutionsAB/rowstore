# Dataset Corrupt CSV Error Handling Specification

## Overview

Tests for handling corrupt/malformed CSV files. Verifies that RowStore properly detects and reports errors in CSV structure.

## Prerequisites

- Running RowStore instance at base URL (default: `http://localhost:8282/`)
- Test data file: `data/dataset4_corrupt.csv`

## Test Data

**File:** `dataset4_corrupt.csv`
- Encoding: UTF-8
- Delimiter: Comma (`,`)
- Issue: First row (header) has fewer columns than subsequent rows

**Content:**
```csv
Column 1,Column 2,Column 3
Value 1a,Value 1b,Value 1c,Value 1d
Value 2a,Value 2b,Value 2c
Value 3a,Value 3b,Value 3c
Value 4a,Value 4b,Value 4c
Value 5a,Value 5b,Value 5c
```

**Note:** The header defines 3 columns, but the first data row has 4 values.

## Test Cases

### TC-DATASET4-001: Submit corrupt CSV

**Request:**
- Method: `POST`
- URL: `{baseUrl}/datasets`
- Headers:
  - `Content-Type: text/csv`
- Body: Contents of `dataset4_corrupt.csv`

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
1. Response status is 202 (file accepted for processing)

**Notes:**
- The file is accepted initially; error occurs during async ETL processing
- Status will change to ERROR (4) after processing attempt

---

### TC-DATASET4-002: Verify error status

**Depends on:** TC-DATASET4-001

**Request:**
- Method: `GET`
- URL: `{datasetUrl}/info` (from TC-DATASET4-001 response)
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Content-Type: `application/json`
- Body:
  ```json
  {
    "status": 4
  }
  ```

**Assertions:**
1. Response status is 200
2. `status` equals 4 (ERROR)

**Retry Logic:**
- Initial delay: 5000ms
- Retry count: 2
- Retry delay: 2500ms

## EtlStatus Reference

- `0` - CREATED: Dataset entry created, no data yet
- `1` - ACCEPTED_DATA: Data received, waiting for processing
- `2` - PROCESSING: ETL in progress
- `3` - AVAILABLE: Processing complete, data available
- `4` - ERROR: Processing failed

## Summary

This test verifies that:
1. Corrupt CSV files are accepted for processing (async model)
2. Processing errors result in status = 4 (ERROR)
3. The info endpoint correctly reports the error status
