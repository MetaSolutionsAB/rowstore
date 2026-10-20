# Export Endpoint Tests

## Overview

Tests for the `/dataset/{id}/export` endpoint which provides JSON and CSV export functionality.

## Prerequisites

- Dataset with known data (UTF-8 encoding, 5 rows)
- Test data: `dataset1_utf8.csv`; `dataset_export_edge.csv` for TC-EXPORT-010 to TC-EXPORT-014

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

### TC-EXPORT-010: CSV export keeps duplicate cell values in their columns

**Test data:** `dataset_export_edge.csv` (header `a,b,c`; rows `x,x,y`, `p,q` and `,,z`)

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/export`
- Headers: `Accept: text/csv`, then `Accept: application/json`

**Assertions:**
- CSV body is parsed with a CSV parser; the header contains `a`, `b`, `c` (in any order) and every row has exactly one cell per header column
- Mapped by header, the row `x,x,y` exports as `a=x`, `b=x`, `c=y` (duplicate values are not collapsed, later columns are not shifted)
- JSON body format is unchanged: `[` + rows joined by `,\n` + `]`, 3 rows, one of them `{"a":"x","b":"x","c":"y"}`

### TC-EXPORT-011: CSV export of a short row yields empty trailing cells

**Test data:** `dataset_export_edge.csv`

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/export`
- Headers: `Accept: text/csv`, then `Accept: application/json`

**Assertions:**
- Status `200`, CSV export completes with all 3 data rows
- Mapped by header, the rows are exactly `{a=x, b=x, c=y}`, `{a=p, b=q, c=""}` (the CSV line `p,q` had no cell for `c`) and `{a="", b="", c=z}`
- JSON export keeps the short row as stored: `{"a":"p","b":"q"}` (no key for the missing cell)

### TC-EXPORT-012: Database failure before streaming returns 500 and releases the connection

**Setup:** Requires direct database access (skipped when testing an external instance). The data table of the
`dataset_export_edge.csv` dataset is renamed so that the export query fails when the row stream is opened, i.e.
before the first byte of the body is written. The table is renamed back afterwards.

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/export`
- Headers: `Accept: application/json`, then `Accept: text/csv`

**Expected Response:**
- Status: `500` for both formats (not `200` with a partial body)

**Assertions:**
- Within 10 seconds no database backend references the data table any more, i.e. the export's connection was
  closed and not left idle in an aborted transaction

### TC-EXPORT-013: Database failure mid-stream aborts the connection

**Setup:** Requires direct database access (skipped when testing an external instance). A dataset is created from
`dataset_export_edge.csv` and about 50 MB of rows (50,000 rows with a 1,000 character value) are inserted directly
into its data table. This is far more than the server's write buffer and the socket buffers can hold, so while the
client pauses reading, the export is blocked mid-stream with its database cursor open.

**Steps (for `Accept: application/json` and `Accept: text/csv`):**
1. Start the export over HTTP/1.1 and read the first 1024 bytes of the body (status `200`)
2. Find the export's database backend in `pg_stat_activity` (query `SELECT data FROM <data table>`) and terminate it with `pg_terminate_backend()`
3. Read the rest of the body

**Assertions:**
- Reading the rest of the body fails with an I/O error (connection aborted, chunked body not terminated) instead
  of ending cleanly with a truncated but complete-looking body
- Note: once the first bytes have been sent, status line and headers are committed, so a `5xx` status is
  impossible; aborting the connection is the only way to signal the failure to the client

### TC-EXPORT-014: Failed exports return their connections to the pool

**Setup:** Implemented in `ConnectionPoolIT` (HikariCP enabled with `connectionPoolMax=3`, requires direct database
access). The data table of a `dataset_export_edge.csv` dataset is renamed as in TC-EXPORT-012.

**Steps:**
1. Export the dataset 4 times (more than the pool size), alternating `Accept: application/json` and `Accept: text/csv`
2. Rename the data table back and export as JSON

**Assertions:**
- Each failed export returns `500`
- The final export returns `200` with all 3 rows (the pool is not exhausted by leaked connections)
- Metric `hikaricp.connections.active` drops to `0` within 10 seconds
