# CSV Format Tests

## Overview

Tests for various CSV format variations: different delimiters, encodings, and edge cases in CSV parsing.

The empty line tests (TC-CSV-011 to TC-CSV-022) use inline CSV content. TC-CSV-011 to TC-CSV-016 run with the
default parser (`CsvEmptyLineIT`), TC-CSV-017 to TC-CSV-022 repeat them with `legacyparser: true`
(`CsvEmptyLineLegacyParserIT`, requires a separate RowStore instance and is skipped when testing an external
instance). See ETL-4.05.

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

## Empty Line Tests

In the CSV content below, `\n` denotes a line break.

### TC-CSV-011: An empty line between rows fails the load and a failed replace keeps the previous data

**Steps:**
1. Create a dataset from `name,value\nA1,1\nA2,2\n` and wait until it is available
2. `PUT /dataset/{id}` with `name,value\nB1,1\nB2,2\n\nB3,3\nB4,4\n`

**Assertions:**
1. The status becomes ERROR (4); the line number of the empty line (4) is logged at ERROR level
2. Queries return exactly the rows `{name: A1, value: 1}`, `{name: A2, value: 2}`

### TC-CSV-012: An empty line directly after the header fails the load

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: `name,value\n\nA1,1\nA2,2\n`

**Assertions:**
1. The status becomes ERROR (4)

### TC-CSV-013: Empty lines at the end of the file are ignored

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: `name,value\nA1,1\nA2,2\n\n\n`

**Assertions:**
1. The status becomes AVAILABLE (3)
2. Queries return exactly the rows `{name: A1, value: 1}`, `{name: A2, value: 2}` (no rows for the empty lines)

### TC-CSV-014: An empty line inside a quoted value is part of the value

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: `name,value\n"first\n\nthird",1\nA2,2\n`

**Assertions:**
1. The status becomes AVAILABLE (3)
2. Queries return exactly the rows `{name: "first\n\nthird", value: 1}`, `{name: A2, value: 2}`

### TC-CSV-015: A line consisting of whitespace only is a data row

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: `name,value\nA1,1\n \nA2,2\n` (the third line is a single space)

**Assertions:**
1. The status becomes AVAILABLE (3)
2. Queries return exactly the rows `{name: A1, value: 1}`, `{name: " "}`, `{name: A2, value: 2}`

### TC-CSV-016: A quoted empty value in a single-column CSV is a data row

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: `name\nA1\n""\nA2\n`

**Assertions:**
1. The status becomes AVAILABLE (3)
2. Queries return exactly the rows `{name: A1}`, `{name: ""}`, `{name: A2}`

### TC-CSV-017 to TC-CSV-022: Empty lines with the legacy parser

As TC-CSV-011 to TC-CSV-016 (in this order), with a RowStore instance configured with `legacyparser: true`. The
results must be the same as with the default parser; in particular, a quoted empty value (TC-CSV-022) must not be
taken for an empty line, although the legacy parser returns the same record for both.
