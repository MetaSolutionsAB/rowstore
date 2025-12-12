# Test Specifications

This folder contains test specifications for the RowStore REST API. These specifications are framework-agnostic and can be used to implement tests in any language or test framework (Java, Kotlin, Groovy, JavaScript, etc.).

## Structure

| File | Description |
|------|-------------|
| `01-status.md` | Status endpoint (`/status`) |
| `02-datasets-list.md` | Datasets listing endpoint (`/datasets`) |
| `03-dataset-utf8-lifecycle.md` | Full dataset lifecycle: create, info, queries, aliases |
| `04-dataset-semicolon.md` | Semicolon-delimited CSV handling |
| `05-dataset-encoding-append-replace.md` | Windows-1252 encoding, append, and replace operations |
| `06-dataset-corrupt.md` | Error handling for corrupt CSV files |
| `07-dataset-empty-column.md` | Handling CSV with empty column labels |

## Prerequisites

- Running RowStore instance (default: `http://localhost:8282/`)
- Test data files in `tests/data/`
- For a clean test run: empty/uninitialized RowStore instance

## Test Data Files

| File | Encoding | Delimiter | Rows | Notes |
|------|----------|-----------|------|-------|
| `dataset1_utf8.csv` | UTF-8 | Comma | 5 | Standard test data |
| `dataset2_utf8_semicolon.csv` | UTF-8 | Semicolon | 5 | Alternative delimiter |
| `dataset3_windows1252.csv` | Windows-1252 | Comma | 5 | Legacy encoding |
| `dataset4_corrupt.csv` | UTF-8 | Comma | 5 | Malformed (column mismatch) |
| `dataset5_utf8_emptycolumn.csv` | UTF-8 | Comma | 5 | Empty column labels |

## Specification Format

Each specification file contains:

- **Overview**: Purpose of the test group
- **Prerequisites**: Required setup
- **Test Data**: Description of input data
- **Test Cases**: Individual tests with:
  - Test ID (e.g., `TC-DATASET1-001`)
  - Dependencies on other tests
  - Request details (method, URL, headers, body)
  - Expected response (status, headers, body structure)
  - Assertions to verify
  - Retry logic (for async operations)

## Dependency Groups

Tests are organized into independent groups that can run in parallel:

1. **Status** (`01-status.md`) - Independent
2. **Datasets List** (`02-datasets-list.md`) - Independent (requires empty instance)
3. **UTF-8 Lifecycle** (`03-dataset-utf8-lifecycle.md`) - Sequential chain
4. **Semicolon CSV** (`04-dataset-semicolon.md`) - Sequential chain
5. **Encoding/Append/Replace** (`05-dataset-encoding-append-replace.md`) - Sequential chain
6. **Corrupt CSV** (`06-dataset-corrupt.md`) - Sequential chain
7. **Empty Column** (`07-dataset-empty-column.md`) - Sequential chain

## Async Operations

Dataset creation and modification are asynchronous. Tests should:

1. Wait for initial delay after submitting CSV (recommended: 5000ms)
2. Retry failed assertions (recommended: 2 retries with 2500ms delay)
3. Check `status` field: wait until status = 3 (AVAILABLE) or 4 (ERROR)

## EtlStatus Values

| Value | Constant | Description |
|-------|----------|-------------|
| 0 | CREATED | Dataset entry created |
| 1 | ACCEPTED_DATA | Data received, pending processing |
| 2 | PROCESSING | ETL in progress |
| 3 | AVAILABLE | Ready for queries |
| 4 | ERROR | Processing failed |

## Implementation Notes

When implementing these tests in a new framework:

1. Use a configurable base URL
2. Implement retry logic for async operations
3. Store dataset URLs from creation responses for subsequent tests
4. Handle URL encoding for Unicode characters in query parameters
5. Tests within a chain must run sequentially; chains can run in parallel
