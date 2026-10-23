# Test Specification: HTTP Compatibility with 1.x

Content negotiation and method tunnelling as in RowStore 1.x (Restlet). Implemented in `ContentNegotiationIT.java`
and `MethodTunnelIT.java`; the negotiation rules are also covered by `ContentNegotiationTest.java` and
`TunnelFilterTest.java` (unit tests).

## Content Negotiation

### TC-CONNEG-001: JSON resources respond with JSON to any Accept header

Spec: API-4.04

**Requests** (status `200`, Content-Type `application/json`):

| Path | Accept |
|------|--------|
| `/dataset/{id}/info` | `application/ld+json`, `text/html`, `text/plain`, `application/xml` |
| `/datasets` | `text/plain`, `text/html` |
| `/status` | `text/html` |
| `/status?jvm` | `text/json` |
| `/dataset/{id}/aliases` | `application/xml` |
| `/dataset/{id}/swagger` | `text/html` |
| `/dataset/{id}/json` | `text/html`, `text/csv` |
| `/dataset/{id}` | `application/ld+json`, `application/xml` |

### TC-CONNEG-002: The dataset resource redirects to the HTML view for text types

Spec: API-4.03, API-4.04

**Requests:** `GET /dataset/{id}` without Accept header and with `text/plain`, `text/csv`, `text/json`,
`text/javascript`, `*/*` and a browser's Accept header

**Expected Response:**
- Status: `303`, `Location`: `{datasetUrl}/html`

### TC-CONNEG-003: The export selects JSON or CSV as in 1.x

Spec: API-3.10, API-4.04

| Accept | Content-Type |
|--------|--------------|
| (none), `*/*`, `application/xml`, `text/csv;q=0.5, application/json` | `application/json` |
| `text/csv`, `text/html`, `text/plain` | `text/csv` |

### TC-CONNEG-004: The HTML view is always served as text/html

Spec: API-3.12, API-4.04

**Requests:** `GET /dataset/{id}/html` without Accept header and with `*/*`, `application/json`, `text/plain`, `text/html`

**Expected Response:**
- Status: `200`, Content-Type `text/html;charset=UTF-8`

### TC-CONNEG-005: The media parameter selects the representation of the dataset resource

Spec: API-4.05

1. `GET /dataset/{id}?media=html` with `Accept: application/json` → `303`
2. `GET /dataset/{id}?media=json&Name=McLoud` with `Accept: text/html` → `200`, exactly McLoud

## Method Tunnel

### TC-TUNNEL-001: POST with method=PUT replaces the data

Spec: API-4.06

**Steps** (dataset with 2 rows):
1. `POST /dataset/{id}?method=PUT` with the same 2 rows → `202`; once available, `rowcount` is 2
2. `POST /dataset/{id}` with the same 2 rows → `202`; once available, `rowcount` is 4

### TC-TUNNEL-002: POST with X-HTTP-Method-Override: DELETE deletes the aliases

Spec: API-4.06

**Steps:**
1. `PUT /dataset/{id}/aliases` with an alias → `204`
2. `POST /dataset/{id}/aliases` with `X-HTTP-Method-Override: DELETE` and the body `["other"]` → `204`
3. `GET /dataset/{id}/aliases` returns `[]`

### TC-TUNNEL-003: The method of a GET request is never tunnelled

Spec: API-4.06, SEC-9.05

1. `GET /dataset/{id}?method=DELETE&name=a` with `X-HTTP-Method-Override: DELETE` → `400` (`method` is not a column)
2. `GET /dataset/{id}` with `X-HTTP-Method-Override: DELETE` → `200`
3. The dataset still exists

### TC-TUNNEL-004: POST with an unsupported tunnelled method is rejected with 400

Spec: API-4.06

1. `POST /dataset/{id}?method=PATCH` with CSV → `400`
2. `POST /dataset/{id}` with `X-HTTP-Method-Override: TRACE` and CSV → `400`
3. The dataset still has its 2 rows (nothing was appended)
