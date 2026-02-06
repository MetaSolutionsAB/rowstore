# Rate Limiting Tests

## Overview

Tests for rate limiting functionality (HTTP 429 responses). Requires a RowStore instance configured with rate limiting enabled.

## Configuration Requirements

Rate limiting must be enabled in `rowstore.json`:

```json
{
  "ratelimit": {
    "timeRange": 60,
    "requestsGlobal": 100,
    "requestsDataset": 10,
    "method": "slidingwindow"
  }
}
```

## Test Cases

### TC-RATE-001: Global rate limit exceeded

**Setup:**
- Configure `requestsGlobal=5` for testing

**Request:**
- Method: `GET`
- URL: `/status` (6+ times rapidly)

**Expected Response:**
- First 5 requests: `200`
- 6th+ request: `429 Too Many Requests`

**Assertions:**
1. 429 status code returned after limit exceeded
2. Error message indicates rate limit

### TC-RATE-002: Per-dataset rate limit exceeded

**Setup:**
- Configure `requestsDataset=3` for testing

**Request:**
- Method: `GET`
- URL: `/dataset/{id}` (4+ times rapidly)

**Expected Response:**
- First 3 requests: `200`
- 4th+ request: `429 Too Many Requests`

**Assertions:**
1. 429 status code returned after per-dataset limit
2. Different datasets have independent limits

### TC-RATE-003: Retry-After header present

**Request:**
- Trigger rate limit (any method)

**Expected Response:**
- Status: `429`
- Header: `Retry-After` present

**Assertions:**
1. `Retry-After` header contains timestamp or seconds
2. Value indicates when client can retry

### TC-RATE-004: Status endpoint exempt from rate limiting

**Setup:**
- Rate limit active (other endpoints returning 429)

**Request:**
- Method: `GET`
- URL: `/status`

**Expected Response:**
- Status: `200`

**Assertions:**
1. Status endpoint always accessible
2. Allows monitoring even under rate limit

### TC-RATE-005: Rate limit reset after time window

**Setup:**
- Trigger rate limit
- Wait for `timeRange` seconds

**Request:**
- Method: `GET`
- URL: `/dataset/{id}`

**Expected Response:**
- Status: `200`

**Assertions:**
1. After time window expires, requests succeed again
2. Rate limit counter resets

## Rate Limit Methods

### Sliding Window

With `method: "slidingwindow"`:
- Requests counted in rolling time window
- Smoother rate limiting
- More memory usage

### Average

With `method: "average"`:
- Requests averaged over time period
- Allows bursts followed by cooldown
- Less memory usage

## Notes

- Rate limiting is optional and disabled by default
- Tests in this spec require a dedicated test profile
- Run with: `mvn verify -Dgroups=ratelimit`
- Do not run against production instances
