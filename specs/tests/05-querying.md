# Querying Tests

## Overview

Tests for query functionality including filters, pagination, and regex modes. The former JSONP callback tests are retired.

## Prerequisites

- Dataset with known data (UTF-8 encoding, 5 rows)
- Test data: `dataset1_utf8.csv`, `dataset_query_edge.csv`

## Basic Query Tests

### TC-QUERY-001: Multi-column filter (AND logic)

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?name=Martinsson&telephone=0`

**Expected Response:**
- Status: `200`
- `results` array matches BOTH filter conditions
- `resultCount` reflects filtered count

### TC-QUERY-002: Empty string filter value

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?comment=`

**Expected Response:**
- Status: `200` or `400`
- If 200: matches rows with empty field

### TC-QUERY-003: Filter with special SQL characters

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?comment=test'--`

**Expected Response:**
- Status: `200`
- No SQL injection (properly escaped)

### TC-QUERY-004: Filter with regex metacharacters in data

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?comment=.*`

**Expected Response:**
- Status: `200`
- Results depend on regex mode configuration

### TC-QUERY-005: queryTime field present

**Request:**
- Method: `GET`
- URL: `/dataset/{id}`

**Expected Response:**
- Status: `200`
- Body contains `queryTime` field >= 0

## Pagination Tests

### TC-QUERY-006: Offset beyond result count

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?_offset=1000`

**Expected Response:**
- Status: `200`
- `results` array is empty
- `resultCount` shows total count

### TC-QUERY-007: Zero limit

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?_limit=0`

**Expected Response:**
- Status: `200` or `400`
- If 200: empty results or treated as default

### TC-QUERY-008: Negative limit

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?_limit=-1`

**Expected Response:**
- Status: `200` or `400`

### TC-QUERY-009: Negative offset

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?_offset=-1`

**Expected Response:**
- Status: `200` (treated as 0) or `400`

### TC-QUERY-010: Non-numeric limit

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?_limit=abc`

**Expected Response:**
- Status: `400`

### TC-QUERY-011: Prev/next links on first page

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?_limit=2`

**Expected Response:**
- Status: `200`
- If `next` present: contains correct offset/limit
- `prev` absent when offset=0

### TC-QUERY-012: Prev/next links on middle page

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?_limit=2&_offset=2`

**Expected Response:**
- Status: `200`
- Both `prev` and `next` links present (if more pages exist)

### TC-QUERY-013: Prev/next links on last page

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?_limit=2&_offset=4`

**Expected Response:**
- Status: `200`
- `prev` link present
- `next` absent when offset+limit >= resultCount

### TC-QUERY-014: Format parameter

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?format=application/json`

**Expected Response:**
- Status: `200`
- Content-Type: `application/json`

## JSONP Callback Tests

JSONP support has been removed (API-7.01, QUERY-3.03), so these test cases are retired. A `_callback` parameter is treated like any other query parameter that is not a column name (QUERY-2.02).

### TC-JSONP-001: [REMOVED] Query with _callback wraps response

### TC-JSONP-002: [REMOVED] Callback with valid JS function name

### TC-JSONP-003: [REMOVED] Empty _callback behavior

### TC-JSONP-004: [REMOVED] Callback on info endpoint

### TC-JSONP-005: [REMOVED] Callback combined with query filters

## Regex Mode Tests (Full Mode)

### TC-REGEX-FULL-001: Regex pattern without prefix

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=Åke.*`

**Expected Response:**
- Status: `200`
- Results include "Åkesson"

### TC-REGEX-FULL-002: Alternation pattern

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=(Å|é)`

**Expected Response:**
- Status: `200`
- Results include "Åkesson" and "Béringer"

### TC-REGEX-FULL-003: Tilde prefix for regex

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=~Åke.*`

**Expected Response:**
- Status: `200`
- Results include "Åkesson"

## Regex Mode Tests (Disabled Mode)

Requires: `regexpqueries=disabled`

### TC-REGEX-DIS-001: Exact match works

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=Åkesson`

**Expected Response:**
- Status: `200`
- Exact match returned

### TC-REGEX-DIS-002: Regex pattern treated literally

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=.*`

**Expected Response:**
- Status: `200`
- Matches literal ".*" only (or no results)

### TC-REGEX-DIS-003: Tilde prefix ignored

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=~Åke`

**Expected Response:**
- Status: `200`
- Matches literal "~Åke" only (or no results)

### TC-REGEX-DIS-004: Swagger shows regex disabled

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/swagger`

**Assertions:**
- Response contains "Exact matching"
- Response does NOT contain "Regular expressions may be used"

### TC-REGEX-DIS-005: Partial match does not work

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=Åk`

**Expected Response:**
- Status: `200`
- `results`: empty (no partial matching without regex)

## Regex Mode Tests (Simple Mode)

Requires: `regexpqueries=simple`

Note: In simple mode, only the caret (^) prefix triggers regex interpretation.
The tilde (~) prefix is NOT supported in simple mode (only in full mode).

### TC-REGEX-SIM-001: Pattern without ^ treated as exact

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=Åke`

**Expected Response:**
- Status: `200`
- Matches "Åke" exactly only (no results if only "Åkesson" exists)

### TC-REGEX-SIM-002: Pattern with ^ triggers regex

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=^Åke.*`

**Expected Response:**
- Status: `200`
- Results include "Åkesson"

### TC-REGEX-SIM-003: Tilde prefix NOT supported in simple mode

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=~Åke`

**Expected Response:**
- Status: `200`
- `results`: empty (tilde treated as literal character)

Note: Tilde (~) only triggers regex in FULL mode, not SIMPLE mode.

### TC-REGEX-SIM-004: Plain pattern does not match partial

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=Åke`

**Expected Response:**
- Status: `200`
- `results`: empty (exact match only, no partial matching)

### TC-REGEX-SIM-005: Alternation pattern with caret

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?Name=^(Åkesson|Béringer)`

**Expected Response:**
- Status: `200`
- Results include matched rows

### TC-REGEX-SIM-006: Swagger shows regex enabled in simple mode

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/swagger`

**Assertions:**
- Response contains "Regular expressions may be used"
