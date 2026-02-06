# Test Specifications

This folder contains test specifications for the RowStore REST API. These specifications are framework-agnostic and can be used to implement tests in any language or test framework (Java, Kotlin, Groovy, JavaScript, etc.).

## Structure

| File | Description | Test Count |
|------|-------------|------------|
| `01-status.md` | Status endpoint (`/status`) including JVM metrics | 5 |
| `02-datasets.md` | Datasets listing endpoint (`/datasets`) | 2 |
| `03-dataset-lifecycle.md` | Full dataset lifecycle: create, info, queries, aliases | 21 |
| `04-csv-formats.md` | CSV format variations: delimiters, encodings, edge cases | 20 |
| `05-querying.md` | Query functionality: filters, pagination, JSONP, regex modes | 24 |
| `06-export.md` | JSON/CSV export endpoints | 8 |
| `07-metadata-endpoints.md` | Swagger and WebGui endpoints | 10 |
| `08-error-handling.md` | Error codes: 400, 404, 423, 424 | 13 |
| `09-rate-limiting.md` | Rate limiting (429) - requires separate profile | 5 |

## Prerequisites

- Running RowStore instance (default: `http://localhost:8282/`)
- Test data files in `webapp/src/test/resources/data/`
- Docker (for Testcontainers-based tests)
- For rate limiting tests: instance with rate limiting configured

## Test Data Files

| File | Encoding | Delimiter | Rows | Notes |
|------|----------|-----------|------|-------|
| `dataset1_utf8.csv` | UTF-8 | Comma | 5 | Standard test data |
| `dataset2_utf8_semicolon.csv` | UTF-8 | Semicolon | 5 | Alternative delimiter |
| `dataset3_windows1252.csv` | Windows-1252 | Comma | 5 | Legacy encoding |
| `dataset4_corrupt.csv` | UTF-8 | Comma | 5 | Malformed (column mismatch) |
| `dataset5_utf8_emptycolumn.csv` | UTF-8 | Comma | 5 | Empty column labels |
| `headers_only.csv` | UTF-8 | Comma | 0 | Only headers, no data |
| `duplicate_cols.csv` | UTF-8 | Comma | 2 | Duplicate column names |
| `long_fields.csv` | UTF-8 | Comma | 2 | Fields > 256 chars |
| `embedded_newlines.csv` | UTF-8 | Comma | 2 | Newlines in quoted fields |
| `whitespace_headers.csv` | UTF-8 | Comma | 2 | Whitespace in headers |
| `quoted_delim.csv` | UTF-8 | Comma | 2 | Delimiters in quoted fields |
| `dataset_query_edge.csv` | UTF-8 | Comma | 5 | Edge case data for queries |

## Test Profiles

Tests are organized into profiles based on server configuration:

| Profile | Configuration | Description |
|---------|---------------|-------------|
| Default | `regexpqueries=full` | Standard tests (105 tests) |
| `regex-disabled` | `regexpqueries=disabled` | Regex disabled mode (4 tests) |
| `regex-simple` | `regexpqueries=simple` | Simple regex mode (4 tests) |
| `ratelimit` | Rate limiting enabled | Rate limit tests (5 tests) |

### Running Different Profiles

```bash
# Run default tests
mvn verify

# Exclude non-default profile tests
mvn verify -DexcludedGroups="regex-disabled,regex-simple,ratelimit"

# Run specific profile (requires matching server config)
mvn verify -Dgroups=regex-disabled
```

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

## API Endpoint Coverage

| Endpoint | Methods | Spec File |
|----------|---------|-----------|
| `/status` | GET | 01-status.md |
| `/status?jvm` | GET | 01-status.md |
| `/datasets` | GET, POST | 02-datasets.md, 03-dataset-lifecycle.md |
| `/dataset/{id}` | GET, POST, PUT, DELETE | 03-dataset-lifecycle.md, 05-querying.md |
| `/dataset/{id}/info` | GET | 03-dataset-lifecycle.md |
| `/dataset/{id}/aliases` | GET, POST, PUT, DELETE | 03-dataset-lifecycle.md |
| `/dataset/{id}/export` | GET | 06-export.md |
| `/dataset/{id}/swagger` | GET | 07-metadata-endpoints.md |
| `/dataset/{id}/html` | GET | 07-metadata-endpoints.md |

## Error Code Coverage

| Code | Description | Spec File |
|------|-------------|-----------|
| 400 | Bad Request | 08-error-handling.md |
| 404 | Not Found | 08-error-handling.md |
| 423 | Locked | 08-error-handling.md |
| 424 | Failed Dependency | 08-error-handling.md |
| 429 | Too Many Requests | 09-rate-limiting.md |

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

## Test Summary

| Category | Test Count |
|----------|------------|
| Status | 5 |
| Datasets List | 2 |
| Dataset Lifecycle | 21 |
| CSV Formats | 20 |
| Querying | 24 |
| Export | 8 |
| Metadata Endpoints | 10 |
| Error Handling | 13 |
| Rate Limiting | 5 |
| **Total** | **108** |

Note: Some tests are shared between Java implementations (105 in default profile) and specifications (108 total including all profiles).

## Implementation Notes

When implementing these tests in a new framework:

1. Use a configurable base URL
2. Implement retry logic for async operations
3. Store dataset URLs from creation responses for subsequent tests
4. Handle URL encoding for Unicode characters in query parameters
5. Tests within a chain must run sequentially; chains can run in parallel
6. Profile-specific tests require matching server configuration
