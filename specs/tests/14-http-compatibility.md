# Test Specification: HTTP Compatibility with 1.x

Content negotiation, writes to `/dataset/{id}/json`, method tunnelling and the decoding of compressed request bodies as
in RowStore 1.x (Restlet), and the handling of `Range` requests. Implemented in `ContentNegotiationIT.java`,
`DatasetJsonPathIT.java`, `MethodTunnelIT.java`, `CompressedUploadIT.java`, `UploadSizeLimitIT.java` and
`RangeRequestIT.java`; the negotiation rules are also covered by `ContentNegotiationTest.java` and
`TunnelFilterTest.java`, the decoding by `RequestDecodingFilterTest.java` and `DatasetUtilTempFileTest.java` (unit
tests).

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
| (none), `*/*`, `application/xml`, `text/csv;q=0.5, application/json`, `text/html, application/json;q=0.9`, `;` | `application/json` |
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
(accept extension), `application/json;q=2, text/html` (invalid quality ends the reading) and `;` (empty media type
ends the reading)

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

## Range Requests

### TC-RANGE-001: Range is ignored

Spec: API-10.01

**Requests** with `Range: bytes=0-9`: `/dataset/{id}`, `/dataset/{id}/json`, `/dataset/{id}/info`,
`/dataset/{id}/export`, `/datasets`, `/status`; and `/dataset/{id}/export` with the open-ended `Range: bytes=5-`

**Expected Response:**
- Status: `200` (not `206`)
- No `Accept-Ranges` and no `Content-Range` header
- The full body; for the export, the same body as without `Range`

## Compressed Request Bodies

Spec: API-3.17, ETL-9.02 (ROWS-173). The bodies are encoded by the test with the JDK's compressors
(`ContentCodings.java`); the CSV is `name,value\nA1,1\nA2,2\n` unless stated otherwise.

### TC-ENCODING-001: A gzip-compressed upload creates the dataset from the decoded CSV

1. `POST /datasets` with `dataset1_utf8.csv` compressed with gzip and `Content-Encoding: gzip` → `202`
2. The dataset becomes AVAILABLE with `rowcount` 5 and 4 column names; `?name=McLoud` returns its row

### TC-ENCODING-002: Uploads with the other supported codings are decoded

For each `Content-Encoding` `x-gzip`, `deflate` (zlib), `deflate-no-wrap` (raw deflate), `zip` (archive with two
entries, the first is loaded), `identity`, `gzip, identity` and `GZIP`:
1. `POST /datasets` with the CSV encoded accordingly → `202`
2. The dataset becomes AVAILABLE with exactly the rows `{name: A1, value: 1}`, `{name: A2, value: 2}`

### TC-ENCODING-003: A gzip-compressed replace (PUT) and append (POST) load the decoded CSV

1. `PUT /dataset/{id}` with gzip-compressed rows `B1` to `B3` → `202`; once AVAILABLE, the rows are `B1`, `B2`, `B3`
2. `POST /dataset/{id}` with gzip-compressed rows `A1`, `A2` → `202`; once AVAILABLE, the rows are `B1` to `B3`,
   `A1`, `A2`

### TC-ENCODING-004: An unsupported coding is rejected with 415 and nothing is stored

1. `POST /datasets` with a plain CSV body, `Content-Encoding: br` and `Origin: https://app.example.com` → `415` with
   `Access-Control-Allow-Origin` and `Accept-Encoding: gzip, x-gzip, deflate, deflate-no-wrap, zip, identity`
2. `PUT /dataset/{id}` with `Content-Encoding: zstd` → `415`; `POST /dataset/{id}` with `Content-Encoding: gzip, br`
   → `415`
3. `PUT /dataset/{id}` with the CSV compressed three times with gzip and `Content-Encoding: gzip, gzip, gzip` → `415`
   (more than two compressions)
4. No dataset was created; the dataset is AVAILABLE with its rows `A1`, `A2`

### TC-ENCODING-005: Corrupt compressed data is rejected with 400 and nothing is stored

1. `POST /datasets` with the first half of a gzip body and `Content-Encoding: gzip` → `400`
2. `POST /datasets` with a plain CSV body and `Content-Encoding: gzip` → `400`
3. `PUT /dataset/{id}` with the truncated gzip body (`gzip`) → `400`; `POST /dataset/{id}` with it as `deflate` → `400`
4. No dataset was created; the dataset is AVAILABLE with its rows `A1`, `A2`

### TC-ENCODING-006: A gzip-compressed alias update is decoded

1. `PUT /dataset/{id}/aliases` with the gzip-compressed body `["compressedalias"]`, `Content-Type: application/json`
   and `Content-Encoding: gzip` → `204`
2. `GET /dataset/{id}/aliases` returns `["compressedalias"]`

### TC-ENCODING-007: maxuploadsize limits the decoded size of an upload

Profile `upload-size-limit` (`maxuploadsize` 4096). The large CSV has 16 KB of equal rows, compressed with gzip to
less than 4096 bytes.
1. `POST /datasets` with the large CSV, uncompressed → `413`
2. `POST /datasets` and `PUT /dataset/{id}` with the compressed large CSV and `Content-Encoding: gzip` → `413`
3. No dataset was created; the existing dataset is AVAILABLE with its previous row count
4. `PUT /dataset/{id}` with a compressed CSV whose decoded size is below the limit → `202`, then AVAILABLE

### TC-ENCODING-NEW1: An alias body larger than 1 MiB is rejected with 413, also when compressed

Spec: API-3.09, API-3.17.
1. `PUT /dataset/{id}/aliases` with a body of exactly 1 MiB (1048576 bytes): `["bigalias"` padded with spaces and
   closed with `]` → `204`
2. `PUT /dataset/{id}/aliases` with 64 MiB of zero bytes compressed twice with gzip (a few hundred bytes) and
   `Content-Encoding: gzip, gzip` → `413`
3. `POST /dataset/{id}/aliases` with the body of step 1 and one more space (1 MiB + 1 byte, uncompressed) → `413`
4. `GET /dataset/{id}/aliases` returns `["bigalias"]`

### TC-ENCODING-008: An upload that is empty after decoding, or without body and Content-Length, is rejected with 400

1. `PUT /dataset/{id}`, `POST /dataset/{id}` and `POST /datasets` with a gzip stream of an empty file and
   `Content-Encoding: gzip` → `400`
2. `PUT` and `POST /dataset/{id}` as HTTP/1.0 requests without body and without `Content-Length`, with and without
   `Content-Encoding: gzip` → `400` (sent with `TestUtils.rawRequest()`, since REST Assured always sends
   `Content-Length`)
3. No dataset was created; the dataset is AVAILABLE with its rows `A1`, `A2`

### TC-ENCODING-009: The body of a request other than POST and PUT is not decoded or rejected

1. `GET /datasets` with `Content-Encoding: br` and a CSV body → `200` (as in 1.x)

Unit tests (`RequestDecodingFilterTest`, `DatasetUtilTempFileTest`, without IDs) cover the codings of stacked and
repeated header lines, the hidden `Content-Encoding` and `Content-Length` headers, `getReader()`, 415 with
`Accept-Encoding` for unsupported codings and for more than two compressions or more than eight codings (also over
several header lines), the limit of each decoder's input by `maxuploadsize`, a zip entry whose name is not UTF-8,
requests with `Content-Length: 0` and methods other than POST and PUT passed on unchanged, bodies of unknown
length, malformed data reported as `MalformedContentException`, failures of reading the request passed on as they
are, the closing of the decoders and the request body when the request has been handled or decoding failed with an
error, the deletion of the temp file when writing it fails (also with an error), and the charset detection of an
empty file.
