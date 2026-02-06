# Error Handling Tests

## Overview

Tests for various error scenarios and HTTP error codes.

## HTTP 423 Locked

### TC-ERROR-001: Delete during PROCESSING returns 423 LOCKED

**Request:**
- Method: `DELETE`
- URL: `/dataset/{id}` (immediately after creation, while processing)

**Expected Response:**
- Status: `423` (if still processing), `204` (if already done), or `404`

Note: This is a race condition test - result depends on timing.

## HTTP 424 Failed Dependency

### TC-ERROR-002: Query before data loaded returns 424

**Request:**
- Method: `GET`
- URL: `/dataset/{id}` (immediately after creation)

**Expected Response:**
- Status: `424` (if still processing) or `200` (if already done)

Note: This is a race condition test - result depends on timing.

## HTTP 400 Bad Request

### TC-ERROR-003: Invalid UUID format returns 404

**Request:**
- Method: `GET`
- URL: `/dataset/not-a-valid-uuid`

**Expected Response:**
- Status: `404`

### TC-ERROR-004: PUT aliases with invalid JSON format returns 400

**Request:**
- Method: `PUT`
- URL: `/dataset/{id}/aliases`
- Body: `not valid json`
- Content-Type: `application/json`

**Expected Response:**
- Status: `400`

### TC-ERROR-005: POST empty body to datasets returns 400

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: (empty)
- Content-Type: `text/csv`

**Expected Response:**
- Status: `400` or `500`

### TC-ERROR-006: POST with non-CSV content type

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: `{"test": "data"}`
- Content-Type: `application/json`

**Expected Response:**
- Status: `400`, `415`, or `202` (behavior varies)

## HTTP 404 Not Found

### TC-ERROR-007: Delete non-existent dataset returns 404

**Request:**
- Method: `DELETE`
- URL: `/dataset/{non-existent-uuid}`

**Expected Response:**
- Status: `404`

## Alias Validation

### TC-ERROR-008: Alias with special characters behavior

**Request:**
- Method: `PUT`
- URL: `/dataset/{id}/aliases`
- Body: `["my-alias!"]`

**Expected Response:**
- Status: `204` (accepted) or `400` (invalid characters)

Note: Depends on alias validation rules.

### TC-ERROR-009: Alias that looks like UUID behavior check

**Request:**
- Method: `PUT`
- URL: `/dataset/{id}/aliases`
- Body: `["{uuid-format-string}"]`

**Expected Response:**
- Status: `204` (accepted) or `400` (rejected to prevent confusion)

Note: UUID-like aliases might be rejected to prevent confusion with actual dataset IDs.

## HTTP 404 Not Found (continued)

### TC-ERROR-010: Default route (/) returns 404

**Request:**
- Method: `GET`
- URL: `/`

**Expected Response:**
- Status: `404`

### TC-ERROR-011: Query with non-existent column returns 400

**Request:**
- Method: `GET`
- URL: `/dataset/{id}?nonexistentcolumn=value`

**Expected Response:**
- Status: `400`

### TC-ERROR-012: Info for non-existent dataset returns 404

**Request:**
- Method: `GET`
- URL: `/dataset/{non-existent-uuid}/info`

**Expected Response:**
- Status: `404`

### TC-ERROR-013: Aliases for non-existent dataset returns 404

**Request:**
- Method: `GET`
- URL: `/dataset/{non-existent-uuid}/aliases`

**Expected Response:**
- Status: `404`

### TC-ERROR-014: Export for non-existent dataset returns 404

**Request:**
- Method: `GET`
- URL: `/dataset/{non-existent-uuid}/export`

**Expected Response:**
- Status: `404`

### TC-ERROR-015: Swagger for non-existent dataset returns 404

**Request:**
- Method: `GET`
- URL: `/dataset/{non-existent-uuid}/swagger`

**Expected Response:**
- Status: `404`

### TC-ERROR-016: HTML for non-existent dataset returns 404

**Request:**
- Method: `GET`
- URL: `/dataset/{non-existent-uuid}/html`

**Expected Response:**
- Status: `404`

## Corrupt CSV Error Handling

These tests are in `DatasetCorruptIT.java`.

### TC-DATASET4-001: Corrupt CSV is accepted for processing

**Request:**
- Method: `POST`
- URL: `/datasets`
- Body: corrupt CSV (column count mismatch)

**Expected Response:**
- Status: `202` (accepted for processing)
- Error detected during async ETL

### TC-DATASET4-002: Dataset status shows ERROR after processing

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/info`

**Expected Response:**
- Status: `200`
- Body: `status` = 4 (ERROR)

### TC-DATASET4-003: Query dataset in ERROR state

**Request:**
- Method: `GET`
- URL: `/dataset/{id}` (dataset in ERROR state)

**Expected Response:**
- Status: `200` (may have data from previous operation) or `424`

Note: Application intentionally allows queries on ERROR status datasets because an error
might occur during an update while the dataset still has valid data from before.

### TC-DATASET4-004: Delete dataset in ERROR state succeeds

**Request:**
- Method: `DELETE`
- URL: `/dataset/{id}` (dataset in ERROR state)

**Expected Response:**
- Status: `204`
