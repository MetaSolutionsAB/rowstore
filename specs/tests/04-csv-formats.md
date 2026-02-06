# CSV Format Tests

## Overview

Tests for various CSV format variations: different delimiters, encodings, and edge cases in CSV parsing.

## Test Data Files

| File | Encoding | Delimiter | Rows | Notes |
|------|----------|-----------|------|-------|
| `dataset1_utf8.csv` | UTF-8 | Comma | 5 | Standard test data |
| `dataset2_utf8_semicolon.csv` | UTF-8 | Semicolon | 5 | Alternative delimiter |
| `dataset3_windows1252.csv` | Windows-1252 | Comma | 5 | Legacy encoding |
| `dataset5_utf8_emptycolumn.csv` | UTF-8 | Comma | 5 | Empty column labels |
| `headers_only.csv` | UTF-8 | Comma | 0 | Only headers, no data |
| `duplicate_cols.csv` | UTF-8 | Comma | 2 | Duplicate column names |
| `long_fields.csv` | UTF-8 | Comma | 2 | Fields > 256 chars |
| `embedded_newlines.csv` | UTF-8 | Comma | 2 | Newlines in quoted fields |
| `whitespace_headers.csv` | UTF-8 | Comma | 2 | Whitespace in headers |
| `quoted_delim.csv` | UTF-8 | Comma | 2 | Delimiters in quoted fields |

## Delimiter Detection Tests

### TC-CSV-SEMICOLON-001: Create dataset from semicolon-separated CSV

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: Contents of `dataset2_utf8_semicolon.csv`
- Content-Type: `text/csv`

**Expected Response:**
- Status: `202`
- Body contains `id`, `url`, `info`, `status`

### TC-CSV-SEMICOLON-002: Verify semicolon CSV parsed correctly

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/info`

**Expected Response:**
- Status: `200`
- `rowcount` = 5
- `status` = 3 (AVAILABLE)
- `columnnames` contains expected columns

### TC-CSV-SEMICOLON-003: Query semicolon CSV data

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=Béringer`

**Expected Response:**
- Status: `200`
- `results[0].name` = "Béringer"
- `results[0].comment` = "No, no comment"

## Encoding Detection Tests

### TC-CSV-ENCODING-001: Create dataset from Windows-1252 CSV

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: Contents of `dataset3_windows1252.csv` (binary)
- Content-Type: `text/csv`

**Expected Response:**
- Status: `202`

### TC-CSV-ENCODING-002: Verify Windows-1252 conversion to UTF-8

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=Åkesson`

**Expected Response:**
- Status: `200`
- `results[0].name` = "Åkesson"
- `results[0].comment` = "Another comment with äöå"

Note: Characters like `é`, `Å`, `ä`, `ö`, `å`, `Ü` correctly converted from Windows-1252.

## Append and Replace Operations

### TC-CSV-APPEND-001: Append data to existing dataset

**Request:**
- Method: `POST`
- URL: `/dataset/{id}` (existing dataset)
- Body: Additional CSV data
- Content-Type: `text/csv`

**Expected Response:**
- Status: `202`

### TC-CSV-APPEND-002: Verify row count after append

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/info`

**Expected Response:**
- Status: `200`
- `rowcount` = original + appended rows

### TC-CSV-REPLACE-001: Replace dataset content

**Request:**
- Method: `PUT`
- URL: `/dataset/{id}`
- Body: New CSV data
- Content-Type: `text/csv`

**Expected Response:**
- Status: `202`

### TC-CSV-REPLACE-002: Verify row count after replace

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/info`

**Expected Response:**
- Status: `200`
- `rowcount` = new data rows only

## Edge Case Tests

### TC-CSV-EDGE-001: Headers only, no data rows

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: Contents of `headers_only.csv`

**Expected Response:**
- Status: `202`

**After Processing:**
- `status` = 3 (AVAILABLE)
- `rowcount` = 0

### TC-CSV-EDGE-002: Duplicate column names

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: Contents of `duplicate_cols.csv`

**Expected Response:**
- Status: `202` (accepted) or `400` (rejected)

Note: Behavior depends on implementation (last wins or error).

### TC-CSV-EDGE-003: Very long field value (>256 chars)

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: Contents of `long_fields.csv`

**Expected Response:**
- Status: `202`

**Assertions:**
- Data stored correctly
- Long fields may not be indexed

### TC-CSV-EDGE-004: Field with embedded newlines

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: Contents of `embedded_newlines.csv`

**Expected Response:**
- Status: `202`

**Assertions:**
- RFC4180 parsing handles quoted newlines correctly

### TC-CSV-EDGE-005: Whitespace in headers

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: Contents of `whitespace_headers.csv`

**Expected Response:**
- Status: `202`

**Assertions:**
- Headers trimmed or preserved consistently

### TC-CSV-EDGE-006: Quoted fields with delimiter

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: Contents of `quoted_delim.csv`

**Expected Response:**
- Status: `202`

**Assertions:**
- Delimiter inside quotes preserved as data

### TC-CSV-EDGE-007: Empty column labels

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: Contents of `dataset5_utf8_emptycolumn.csv`

**Expected Response:**
- Status: `202`

**After Processing:**
- `columnnames` length = 4 (empty labels excluded)
- Only named columns included

### TC-CSV-EDGE-008: Query dataset with empty column labels

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=Åkesson`

**Expected Response:**
- Status: `200`
- Dataset queryable despite edge case headers
