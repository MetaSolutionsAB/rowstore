# Metadata Endpoints Tests

## Overview

Tests for metadata endpoints: Swagger API documentation and WebGui HTML interface.

## Swagger Endpoint (`/dataset/{id}/swagger`)

### TC-SWAGGER-001: GET Swagger spec returns valid JSON

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/swagger`
- Headers: `Accept: application/json`

**Expected Response:**
- Status: `200`
- Content-Type: `application/json`
- Body contains: `swagger`, `info`, `paths` fields

### TC-SWAGGER-002: Swagger contains dataset columns as parameters

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/swagger`

**Assertions:**
- Response contains column names from dataset (name, telephone, comment)
- Columns listed as query parameters

### TC-SWAGGER-003: Swagger for non-existent dataset returns 404

**Request:**
- Method: `GET`
- URL: `/dataset/{non-existent-uuid}/swagger`

**Expected Response:**
- Status: `404`

### TC-SWAGGER-004: Swagger contains standard parameters

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/swagger`

**Assertions:**
- Response contains: `_limit`, `_offset`, `_callback`
- Parameters have descriptions

### TC-SWAGGER-005: Swagger contains dataset ID

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/swagger`

**Assertions:**
- Response body contains the dataset ID

### TC-SWAGGER-006: Swagger info section contains version

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/swagger`

**Assertions:**
- `info.version` is present and not null

## WebGui Endpoint (`/dataset/{id}/html`)

### TC-WEBGUI-001: GET HTML page returns text/html

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/html`
- Headers: `Accept: text/html`

**Expected Response:**
- Status: `200`
- Content-Type: `text/html`
- Body is valid HTML (contains `<html`)

### TC-WEBGUI-002: GET embedded HTML returns valid page

**Request:**
- Method: `GET`
- URL: `/dataset/{id}/html?embed`
- Headers: `Accept: text/html`

**Expected Response:**
- Status: `200`
- Content-Type: `text/html`
- Body is non-empty

### TC-WEBGUI-003: HTML for non-existent dataset returns 404

**Request:**
- Method: `GET`
- URL: `/dataset/{non-existent-uuid}/html`

**Expected Response:**
- Status: `404`

### TC-WEBGUI-004: Full HTML differs from embedded HTML

**Request:**
- Compare `/dataset/{id}/html` vs `/dataset/{id}/html?embed`

**Assertions:**
- Both return 200
- Both are valid HTML
- Content may differ (embedded version optimized for iframe)
