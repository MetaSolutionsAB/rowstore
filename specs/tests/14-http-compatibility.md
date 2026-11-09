# Test Specification: HTTP Compatibility with 1.x

Content negotiation, writes to `/dataset/{id}/json` and method tunnelling as in RowStore 1.x (Restlet). Implemented in
`ContentNegotiationIT.java`, `DatasetJsonPathIT.java` and `MethodTunnelIT.java`; the negotiation rules are also covered by
`ContentNegotiationTest.java` and `TunnelFilterTest.java` (unit tests).

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

### TC-CONNEG-002: The dataset resource redirects to the HTML view if HTML matches best

Spec: API-4.03, API-4.04

**Requests:** `GET /dataset/{id}` with `text/html`, `text/plain`, `text/csv`, `text/json`, `text/javascript`,
`text/html, */*`, `text/plain;q=0.3, */*;q=0.5`, `application/json;q=0.9, text/html`, `text` (no subtype),
`text/html, application/json;q=2` (invalid quality ends the reading) and a browser's Accept header
(`text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8`)

**Expected Response:**
- Status: `303`, `Location`: `{datasetUrl}/html`

### TC-CONNEG-003: The export selects JSON or CSV as in 1.x

Spec: API-3.10, API-4.04

| Accept | Content-Type |
|--------|--------------|
| (none), `*/*`, `application/xml`, `text/csv;q=0.5, application/json`, `text/html, application/json;q=0.9` | `application/json` |
| `text/csv`, `text/html`, `text/plain`, `text/html, */*`, `text/plain;q=0.5, */*;q=0.2`, `text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2` | `text/csv` |

### TC-CONNEG-004: The HTML view is always served as text/html

Spec: API-3.12, API-4.04

**Requests:** `GET /dataset/{id}/html` without Accept header and with `*/*`, `application/json`, `text/plain`, `text/html`

**Expected Response:**
- Status: `200`, Content-Type `text/html;charset=UTF-8`

### TC-CONNEG-005: The media parameter selects the representation of the dataset resource

Spec: API-4.05

1. `GET /dataset/{id}?media=html` with `Accept: application/json` → `303`
2. `GET /dataset/{id}?media=json&Name=McLoud` with `Accept: text/html` → `200`, exactly McLoud

### TC-CONNEG-006: Negotiated responses carry Vary: Accept

Spec: API-4.04

1. `GET /dataset/{id}` with `Accept: application/json` (200), `text/html` (303) and without `Accept` (200) → `Vary` contains `Accept`
2. `GET /dataset/{id}/export` with `Accept: text/csv` and `application/json` (200) → `Vary` contains `Accept`
3. `GET /dataset/{unknown}` and `GET /dataset/{unknown}/export` (404) → `Vary` contains `Accept`

### TC-CONNEG-007: The dataset resource responds with JSON without a better match for HTML

Spec: API-4.01, API-4.04

**Requests:** `GET /dataset/{id}` without Accept header and with `*/*`, `*/*;q=0.8`, `image/png`,
`image/gif, */*; q=.2`, `application/xml, */*;q=0.5`, `text/html, application/json`,
`text/html;q=0.9, application/json`, `garbage` (unparseable), `application/json;q=1;foo=bar, text/html;q=0.5`
(accept extension) and `application/json;q=2, text/html` (invalid quality ends the reading)

**Expected Response:**
- Status: `200`, Content-Type `application/json`, `resultCount` 5

## Writes to /dataset/{id}/json

Implemented in `DatasetJsonPathIT.java`.

### TC-JSONPATH-001: POST appends and PUT replaces, also tunnelled, with the Location of the dataset

Spec: API-3.16, API-4.06

**Steps** (dataset with 2 rows; every response: `202`, `Location` and `url` `{datasetUrl}` without `/json`, `info`
`{datasetUrl}/info`):
1. `POST /dataset/{id}/json` with the same 2 rows; once available, `rowcount` is 4
2. `PUT /dataset/{id}/json` with the same 2 rows; once available, `rowcount` is 2
3. `POST /dataset/{id}/json`, then `POST /dataset/{id}/json?method=PUT`; once available, `rowcount` is 2

### TC-JSONPATH-002: DELETE deletes the dataset, also tunnelled

Spec: API-3.16, API-4.06

1. `DELETE /dataset/{id}/json` → `204`; `GET /dataset/{id}/info` → `404`
2. `POST /dataset/{id}/json?method=DELETE` (another dataset) → `204`; `GET /dataset/{id}/info` → `404`
3. `DELETE /dataset/{id}/json` of the deleted dataset → `404`

### TC-JSONPATH-003: OPTIONS lists the same methods as for the dataset resource

Spec: API-3.16

`OPTIONS /dataset/{id}/json` → `200`, `Allow` contains `GET`, `PUT`, `POST` and `DELETE` and equals the `Allow` of
`OPTIONS /dataset/{id}`

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

### TC-TUNNEL-005: A cross-origin POST with a tunnelled method is refused with 403

Spec: API-4.06, SEC-9.05

1. `POST /dataset/{id}?method=DELETE` with `Origin: https://evil.example.com`, `Content-Type: text/plain` → `403`, empty body
2. `POST /dataset/{id}` with `Origin: null`, `X-HTTP-Method-Override: DELETE`, `Content-Type: text/plain` → `403`
3. `POST /dataset/{id}?method=PUT` with `Origin: https://evil.example.com` and CSV → `403`
4. The dataset still has its 2 rows
