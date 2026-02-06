# Datasets List Endpoint Specification

## Overview

Tests for the `/datasets` endpoint which returns a list of all dataset IDs.

## Prerequisites

- Running RowStore instance at base URL (default: `http://localhost:8282/`)
- Empty/uninitialized RowStore instance (no existing datasets)

## Test Cases

### TC-DATASETS-001: Get empty datasets array

**Request:**
- Method: `GET`
- URL: `{baseUrl}/datasets`
- Headers:
  - `Accept: application/json`

**Expected Response:**
- Status Code: `200 OK`
- Content-Type: `application/json`
- Body: `[]` (empty JSON array)

**Assertions:**
1. Response status is 200
2. Content-Type header contains `application/json`
3. Response body is an empty JSON array

**Notes:**
- This test assumes a clean RowStore instance with no datasets
- After datasets are created, this endpoint will return an array of dataset UUIDs
