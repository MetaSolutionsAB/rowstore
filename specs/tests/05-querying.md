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

### TC-QUERY-014: [REMOVED] Format parameter

Retired: the `format` parameter was never implemented (API-4.02, QUERY-3.04 removed).

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?format=application/json`

**Expected Response:**
- Status: `200`
- Content-Type: `application/json`

### TC-QUERY-018: Reserved _callback parameter is ignored

Spec: QUERY-3.05

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?_callback=x`

**Expected Response:**
- Status: `200`
- Content-Type: `application/json`, body is plain JSON (not wrapped in `x(...)`)
- `results`, `resultCount`, `offset`, `limit`, `prev`, `next` equal those of `/dataset/{id}` without `_callback`

### TC-QUERY-019: Reserved _callback parameter is ignored together with column filters

Spec: QUERY-3.05

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/json?_callback=x&Name=McLoud`

**Expected Response:**
- Status: `200`
- Content-Type: `application/json`, body is plain JSON
- `results`, `resultCount`, `offset`, `limit`, `prev`, `next` equal those of `/dataset/{id}/json?Name=McLoud`

### TC-QUERY-020: Reserved _callback parameter is carried over to prev/next links

Spec: QUERY-3.05, QUERY-6.02

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?_callback=x&_limit=2&_offset=2`

**Expected Response:**
- Status: `200`
- `prev` and `next` URLs contain `_callback=x`

### TC-QUERY-021: Unencoded regex characters in the query string are accepted

Spec: QUERY-9.03

**Setup:** `dataset1_utf8.csv`, `regexpqueries=full`. The request line is sent byte for byte (e.g. over a raw socket),
since HTTP clients encode or reject these characters.

**Requests and expected results** (status `200`, the same results as the percent-encoded form):

| Raw query string | Names in `results` |
|------------------|--------------------|
| `Name=^M` | McLoud, Martinsson |
| `Name=^(McLoud\|Martinsson)$` | McLoud, Martinsson |
| `Name=^[MB]` | Béringer, McLoud, Martinsson |
| `Name=^M.{5}$` | McLoud |
| `Name=^Mc\w+` | McLoud |
| `Name=~^(M\|%C3%9C)` | McLoud, Martinsson, Überhuber |

`Name="x"`, ``Name=`x` `` and `Name=<x>` return `200` with no results. Non-ASCII characters must be percent-encoded.

### TC-QUERY-022: A malformed percent-encoding is rejected with 400

Spec: QUERY-9.04

**Requests** (raw query strings, on `/dataset/{id}` and `/dataset/{id}/json`): `Name=100%`, `Name=%zz`, `Name=%C`,
`Name=McLoud&Telephone=%zz`, `Telephone=%zz&Name=McLoud`, `Na%zzme=McLoud`

**Expected Response:**
- Status: `400` (not `200` with the malformed filter dropped)

### TC-QUERY-023: Parameter names and values are trimmed as in 1.x

Spec: QUERY-2.03

**Requests:** `Name=McLoud%20`, `Name=+McLoud`, `Name=%20McLoud%20`, `%20Name=McLoud`, `Name+=McLoud`,
`Name=McLoud&_limit=%201`, `Name=McLoud&%20=x`, `Name=Martinsson&Name=McLoud`

**Expected Response:**
- Status: `200`
- `results` contains exactly McLoud (the last value of a repeated parameter is used)

### TC-QUERY-024: Tunnel parameters of 1.x are not column filters

Spec: QUERY-3.06

**Requests:**
1. `/dataset/{id}?Name=McLoud&media=json&charset=utf-8&language=sv&encoding=identity` → `200`, exactly McLoud
2. `/dataset/{id}?media=json&charset=utf-8&_limit=2&_offset=2` → `200`, 2 results, `prev` and `next` contain neither `media` nor `charset`
3. A dataset with a column `language` (`language,n` / `sv,1` / `en,2`): `?language=sv` → `200`, exactly the row with `n` = `1`

## JSONP Callback Tests

JSONP support has been removed (API-7.01, QUERY-3.03), so these test cases, which expected wrapped responses as in 1.x, are retired. `_callback` is accepted and ignored (QUERY-3.05, see TC-QUERY-018 to TC-QUERY-020).

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

### TC-REGEX-FULL-004: Regular expressions with quantified groups are accepted

Spec: QUERY-4.05

**Requests and expected results** (status `200`):

| Filter | Names in `results` |
|--------|--------------------|
| `Name=^(Mc\|Ma)(L\|r)+` | McLoud, Martinsson |
| `Name=^M[a-z]+(,[0-9]+)*$` | Martinsson |
| `Name=^(M\|B)(c\|é)*` | Béringer, McLoud, Martinsson |
| `Name=~(a\|e)+` | Béringer, Åkesson, Martinsson, Überhuber |

### TC-REGEX-FULL-005: A value longer than 200 characters that is detected as a regex is accepted

Spec: QUERY-4.05

**Setup:** A dataset with the rows `https://example.org/<250 × a>/data.csv,1` and `https://example.org/other,2`
(columns `url,n`).

**Request:** `GET /dataset/{id}?url=https://example.org/<250 × a>/data.csv` (detected as a regex because of `/`)

**Expected Response:**
- Status: `200`
- `results`: exactly the row with `n` = `1`

### TC-REGEX-FULL-006: An invalid regular expression is rejected with 400

Spec: QUERY-4.05

**Request:** `GET /dataset/{id}?Name=^(M`

**Expected Response:**
- Status: `400`

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

### TC-REGEX-SIM-007: Regular expressions with quantified groups are accepted

Spec: QUERY-4.05

**Request:** `GET /dataset/{id}?Name=^(Mc|Ma)(L|r)+`

**Expected Response:**
- Status: `200`
- Results: McLoud, Martinsson

### TC-REGEX-SIM-008: Unencoded regex characters in the query string are accepted

Spec: QUERY-9.03

**Request:** `GET /dataset/{id}?Name=^(McLoud|Martinsson)$`, request line sent byte for byte

**Expected Response:**
- Status: `200`
- Results: McLoud, Martinsson
