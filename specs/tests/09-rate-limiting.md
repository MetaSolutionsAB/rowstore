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

### TC-RATE-009: Spoofed X-Forwarded-For does not bypass the client IP limit

**Setup:**
- `ratelimit.clientip=5`, `global` and `dataset` high (1000), `timerange=30`
- `trustedproxies: []`, so the test client on loopback is not a trusted proxy

**Request:**
- Method: `GET`
- URL: `/datasets` (10 times, i.e. limit + 5)
- Header: `X-Forwarded-For: <random IPv4 address>`, a different value per request

**Assertions:**
1. At most 5 requests return `200`
2. At least one request returns `429` (before ROWS-128 every request got a fresh key and none was limited)

### TC-RATE-010: Clients behind a trusted proxy are limited independently

**Setup:**
- `ratelimit.clientip=5`, `global` and `dataset` high (1000), `timerange=30`
- `trustedproxies: ["127.0.0.1", "::1"]`, so the test client acts as a trusted reverse proxy

**Requests and assertions (`GET /datasets`, in order):**
1. 5 × `X-Forwarded-For: 10.0.0.1` return `200`, the 6th returns `429`
2. `X-Forwarded-For: 10.0.0.2` returns `200` (independent limit)
3. `X-Forwarded-For: <random>, 10.0.0.1` returns `429` (keyed on the entry appended by the trusted proxy, the client-supplied left part is ignored)
4. `X-Forwarded-For: <random>, 10.0.0.2` returns `200`
5. `X-Forwarded-For: <random>, 10.0.0.1, 127.0.0.1` returns `429` (trusted hop skipped, keyed on 10.0.0.1)

### TC-CORS-007: Rate-limited responses carry CORS headers

**Setup:** Environment of TC-RATE-009 (`clientip=5`, default `cors.allowedorigins`).

**Request:**
- Method: `GET`
- URL: `/datasets` (until a `429` is returned, at most 6 times)
- Header: `Origin: http://example.com`

**Assertions:**
1. A `429` response is returned
2. It has `Access-Control-Allow-Origin: *`, `Access-Control-Expose-Headers` containing `Retry-After`, and a `Retry-After` header

### TC-RATE-011: Client address is resolved from X-Forwarded-For only via trusted proxies

**Type:** Unit test (`TrustedProxyFilterTest`), no server needed. Specification: SEC-5.05, SEC-5.06.

**Assertions (one case per row):** `getRemoteAddr()` and `getRemoteHost()` seen by the next filter in the chain:

| `trustedproxies` | Peer | `X-Forwarded-For` | Remote address |
|---|---|---|---|
| not set | 127.0.0.1, 198.51.100.1 (any peer) | `203.0.113.7` | `203.0.113.7` |
| not set | 10.1.2.3 | `6.6.6.6, 203.0.113.7` | `203.0.113.7` |
| not set | 10.1.2.3 | `6.6.6.6, 10.9.8.7` (private last entry is not skipped) | `10.9.8.7` |
| not set | 127.0.0.1 | two header lines `6.6.6.6` and `203.0.113.7` | `203.0.113.7` |
| not set | 127.0.0.1 | `6.6.6.6, localhost` (used as written, not resolved) | `localhost` |
| not set | 127.0.0.1 | none, or only separators | 127.0.0.1 |
| private ranges¹ | 127.0.0.1, 10.1.2.3, 172.31.0.1, ::1 | `203.0.113.7` | `203.0.113.7` |
| private ranges¹ | fd00::5 | `2001:db8::1` | `2001:db8::1` |
| private ranges¹ | 172.32.0.1, 198.51.100.1 (not private) | `203.0.113.7` | peer |
| `[]` | 127.0.0.1 | `203.0.113.7` | 127.0.0.1 |
| `["192.0.2.10"]` | 192.0.2.11 | `203.0.113.7` | 192.0.2.11 |
| `["192.0.2.10"]` | 192.0.2.10 | `6.6.6.6, 203.0.113.7` | `203.0.113.7` |
| `["192.0.2.0/24"]` | 192.0.2.10 | `6.6.6.6, 203.0.113.7, 192.0.2.20` | `203.0.113.7` |
| `["192.0.2.10"]` | 192.0.2.10 | two header lines `6.6.6.6` and `203.0.113.7` | `203.0.113.7` |
| `["192.0.2.0/24"]` | 192.0.2.10 | `192.0.2.30, 192.0.2.20` (all trusted) | `192.0.2.30` (leftmost) |
| `["192.0.2.10"]` | 192.0.2.10 | `unknown` | `unknown` |
| private ranges¹ | 127.0.0.1 | `6.6.6.6, localhost` (host name not resolved) | `localhost` |
| private ranges¹ | 127.0.0.1 | `::1%zz`, `203.0.113.9, ::1%zz`, `203.0.113.9, fe80::1%lo` (zone ID, no exception) | rightmost entry as given |
| private ranges¹ | fe80::1%2 (zone ID of the socket peer is ignored) | `203.0.113.7` | `203.0.113.7` |
| private ranges¹ | 127.0.0.1 | `203.0.113.9, 10.0.0.1:4711` (port, not an IP literal) | `10.0.0.1:4711` |
| `["192.0.2.10"]` | 192.0.2.10 | none, or only separators | 192.0.2.10 |

¹ `["127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "100.64.0.0/10", "::1", "fc00::/7", "fe80::/10"]` (Tomcat's `internalProxies`)

### TC-RATE-012: X-Forwarded-For from an untrusted peer is logged once per peer

**Type:** Unit test (`TrustedProxyFilterTest`), no server needed. Specification: SEC-5.05.

**Setup:** `trustedproxies: ["192.0.2.10"]`, WARN events of `TrustedProxyFilter` captured.

**Assertions:**
1. Two requests with `X-Forwarded-For` from untrusted peer 198.51.100.1, one request without the header from 198.51.100.2 and one from the trusted proxy produce exactly one warning, naming 198.51.100.1 and `trustedproxies`
2. A request with the header from 198.51.100.2 produces a second warning
3. With `trustedproxies: []`, and without `trustedproxies` (the connecting peer is trusted), no warning is logged

### TC-RATE-013: Without trustedproxies, the last X-Forwarded-For entry of the connecting peer is the client

**Setup:**
- Same rate limit as TC-RATE-009, without `trustedproxies`, so the test client is trusted as the reverse proxy

**Steps and assertions:**
1. 5 requests with `X-Forwarded-For: <random>, 10.0.0.1` return `200` (the client-supplied left part is ignored)
2. The next request with `X-Forwarded-For: <random>, 10.0.0.1` returns `429`
3. `X-Forwarded-For: 10.0.0.2` returns `200` (another client behind the proxy has its own limit)
4. `X-Forwarded-For: 10.0.0.3, 10.0.0.1` returns `429` (keyed on the last entry; private addresses are not skipped)

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
