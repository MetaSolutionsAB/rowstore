# Export Endpoint Tests

## Overview

Tests for the `/dataset/{id}/export` endpoint which provides JSON and CSV export functionality.

## Prerequisites

- Dataset with known data (UTF-8 encoding, 5 rows)
- Test data: `dataset1_utf8.csv`

## Test Cases

### TC-EXPORT-001: Export as JSON returns valid JSON array

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/export`
- Headers: `Accept: application/json`

**Expected Response:**
- Status: `200`
- Content-Type: `application/json`
- Body: Valid JSON array with 5 elements
- Body contains row data matching original CSV

### TC-EXPORT-002: Export as CSV returns valid CSV

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/export`
- Headers: `Accept: text/csv`

**Expected Response:**
- Status: `200`
- Content-Type: `text/csv`
- Body: Valid CSV with header row + 5 data rows
- Headers include: name, telephone, comment

### TC-EXPORT-003: Export JSON content matches original dataset

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/export`
- Headers: `Accept: application/json`

**Assertions:**
- Response contains expected data values (Åkesson, Béringer)
- Unicode characters preserved correctly
- All 5 rows exported

### TC-EXPORT-004: Export CSV content verification

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/export`
- Headers: `Accept: text/csv`

**Assertions:**
- Response contains expected data values
- Unicode characters (Åkesson, Béringer, äöå) preserved
- CSV properly formatted

### TC-EXPORT-005: Export non-existent dataset returns 404

**Request:**
- Method: `GET`
- URL: `/dataset/{non-existent-uuid}/export`
- Headers: `Accept: application/json`

**Expected Response:**
- Status: `404`

### TC-EXPORT-006: Export dataset with status=CREATED returns 424

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/export` (immediately after creation, before processing)
- Headers: `Accept: application/json`

**Expected Response:**
- Status: `424` (FAILED_DEPENDENCY) or `200` if processing completed quickly

### TC-EXPORT-007: Export preserves UTF-8 characters

**Request:**
- Both JSON and CSV exports

**Assertions:**
- Åkesson preserved correctly
- Béringer preserved correctly
- äöå preserved correctly

### TC-EXPORT-008: Export dataset with empty field values

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/export`
- Uses `dataset_query_edge.csv` which has empty_field column

**Assertions:**
- Empty values exported correctly
- JSON export contains empty strings for empty fields
- CSV export has proper empty field handling

### TC-EXPORT-009: Content-Disposition header (if present)

**Request:**
- Both JSON and CSV exports

**Assertions:**
- If Content-Disposition header present:
  - JSON: filename contains dataset ID and `.json`
  - CSV: filename contains dataset ID and `.csv`
