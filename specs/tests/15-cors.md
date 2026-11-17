# CORS Tests

## Overview

Tests for Cross-Origin Resource Sharing ([SEC-6](../07-security.md#sec-6-cors)): CORS headers and preflight requests
with the default configuration (all origins), and the rejection of origins that are not listed in a restricted
`cors.allowedorigins`. TC-CORS-007 (rate-limited responses carry CORS headers) is specified in
[09-rate-limiting.md](09-rate-limiting.md), since it needs a rate-limited instance.

## Prerequisites

- TC-CORS-001 to TC-CORS-006: instance with the default CORS configuration (`cors.allowedorigins` not set, i.e. all
  origins); `http://example.com` is used as origin (`CorsIT`)
- TC-CORS-008 to TC-CORS-012: instance with `"cors": {"allowedorigins": ["https://app.example.com"]}`
  (`CorsRestrictedIT`)

## Default Configuration (All Origins)

### TC-CORS-001: Preflight OPTIONS returns CORS headers

Spec: SEC-6.01

**Request:**
- Method: `OPTIONS`
- URL: `/status`
- Headers: `Origin: http://example.com`, `Access-Control-Request-Method: GET`

**Expected Response:**
- Status: `200`
- `Access-Control-Allow-Origin` is present

### TC-CORS-002: GET with Origin header includes CORS response headers

Spec: SEC-6.01

**Request:**
- Method: `GET`
- URL: `/status`
- Headers: `Origin: http://example.com`, `Accept: application/json`

**Expected Response:**
- Status: `200`
- `Access-Control-Allow-Origin` is present

### TC-CORS-003: CORS headers on 404 error responses

Spec: SEC-6.01

**Request:**
- Method: `GET`
- URL: `/dataset/{random UUID}`
- Headers: `Origin: http://example.com`, `Accept: application/json`

**Expected Response:**
- Status: `404`
- `Access-Control-Allow-Origin` is present, so that browsers can read the error

### TC-CORS-004: Preflight allows POST method

Spec: SEC-6.01

**Request:**
- Method: `OPTIONS`
- URL: `/datasets`
- Headers: `Origin: http://example.com`, `Access-Control-Request-Method: POST`

**Expected Response:**
- Status: `200`
- `Access-Control-Allow-Methods` contains `POST`

### TC-CORS-005: Preflight allows DELETE method

Spec: SEC-6.01

**Request:**
- Method: `OPTIONS`
- URL: `/datasets`
- Headers: `Origin: http://example.com`, `Access-Control-Request-Method: DELETE`

**Expected Response:**
- Status: `200`
- `Access-Control-Allow-Methods` contains `DELETE`

### TC-CORS-006: CORS exposes Location header

Spec: SEC-6.01

**Request:**
- Method: `OPTIONS`
- URL: `/status`
- Headers: `Origin: http://example.com`, `Access-Control-Request-Method: GET`

**Expected Response:**
- Status: `200`
- `Access-Control-Expose-Headers` contains `Location` (browsers can read the URL of a created dataset)

## Restricted Origins

### TC-CORS-008: A listed origin gets the CORS headers

Spec: SEC-6.01, SEC-6.02

**Requests:**
1. `GET /status` with `Origin: https://app.example.com`, `Accept: application/json`
2. `OPTIONS /datasets` with `Origin: https://app.example.com`, `Access-Control-Request-Method: POST`

**Expected Response:**
- Status: `200`
- `Access-Control-Allow-Origin: https://app.example.com`

### TC-CORS-009: An origin that is not listed is rejected with 403

Spec: SEC-6.02

**Requests:**
1. `GET /status` with `Origin: https://evil.example.com`, `Accept: application/json`
2. `POST /datasets` with `Origin: https://evil.example.com`, `Content-Type: text/csv` and a CSV body
3. `OPTIONS /datasets` with `Origin: https://evil.example.com`, `Access-Control-Request-Method: DELETE`

**Expected Response:**
- Status: `403`
- Request 1: body `Invalid CORS request`, no `Access-Control-Allow-Origin`
- Request 2: no `Location` header (no dataset is created)
- Request 3: no `Access-Control-Allow-Origin`

### TC-CORS-010: Origin null is rejected with 403

Spec: SEC-6.02

**Request:**
- Method: `GET`
- URL: `/status`
- Headers: `Origin: null`, `Accept: application/json`

**Expected Response:**
- Status: `403`

### TC-CORS-011: Requests without Origin are not checked

Spec: SEC-6.02

**Request:**
- Method: `GET`
- URL: `/status`
- Headers: `Accept: application/json` (no `Origin`, as sent by non-browser clients)

**Expected Response:**
- Status: `200`
- No `Access-Control-Allow-Origin`

### TC-CORS-012: Same-origin requests are not checked, another scheme of the same host is rejected

Spec: SEC-6.02

**Requests:**
1. `GET /status` with `Origin: http://localhost:{port}` (the scheme, host and port that RowStore sees)
2. `GET /status` with `Origin: https://localhost:{port}` (as for a page served over https by a TLS-terminating proxy)

**Expected Response:**
- Request 1: status `200`
- Request 2: status `403`, since the origin is neither listed nor the same origin
