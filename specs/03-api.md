# REST API

## Purpose

Documents the RowStore REST API: design principles, endpoint catalog, content negotiation, response formats, and error conventions.

## Scope

Covers all HTTP endpoints, their methods, request/response formats, and status codes. For query filter details, see [05-querying.md](05-querying.md). For ETL processing triggered by uploads, see [04-etl-pipeline.md](04-etl-pipeline.md).

## API-1 Design Principles

> **API-1.01** RowStore follows a RESTful, resource-oriented design. Each endpoint corresponds to a resource (dataset, alias, status) with standard HTTP method semantics.

> **API-1.02** JSON is the default response format for all API endpoints.

> **API-1.03** Content negotiation is supported via the `Accept` header and the `format` query parameter. The `format` parameter takes precedence over the `Accept` header.

> **API-1.04** [REMOVED] JSONP support via `_callback` parameter — replaced by CORS.

## API-2 Base URL

> **API-2.01** The base URL is configured via the `baseurl` setting and is used in response bodies for constructing resource URLs (e.g., `next`/`prev` pagination links, `Location` headers, `@id` in JSON-LD).

## API-3 Endpoint Catalog

> **API-3.01** `GET /status` — Returns service status including name, version, dataset count, and active ETL processes. Accepts optional `?jvm` parameter for JVM memory and processor details. **Exempt from rate limiting.**

> **API-3.02** `GET /datasets` — Returns a JSON array of all dataset UUIDs. Status 200.

> **API-3.03** `POST /datasets` — Creates a new dataset from an uploaded CSV file. Content-Type must be `text/csv`. Returns 202 Accepted with a JSON body containing `id`, `url`, `status`, and `info` URL. Sets `Location` header to the new dataset URL.

> **API-3.04** `GET /dataset/{id}` and `GET /dataset/{id}/json` — Queries a dataset with column-name query parameters as filters. Returns a JSON response envelope with `results`, `limit`, `offset`, `resultCount`, `queryTime`, `prev`, and `next`. The `{id}` can be a UUID or an alias. See [05-querying.md](05-querying.md) for filter details.

> **API-3.05** `POST /dataset/{id}` — Appends CSV data to an existing dataset. Column structure must match. Returns 202 Accepted.

> **API-3.06** `PUT /dataset/{id}` — Replaces all data in an existing dataset with new CSV data. Truncates the data table before loading. Returns 202 Accepted.

> **API-3.07** `DELETE /dataset/{id}` — Deletes a dataset, its data table, and all aliases. Only permitted when status is AVAILABLE (3) or ERROR (4). Returns **204 No Content** on success, 423 Locked if dataset is still processing.

> **API-3.08** `GET /dataset/{id}/info` — Returns dataset metadata in JSON-LD format: status, created timestamp, column names, row count, identifier, aliases, `@context`, and `@id`.

> **API-3.09** `GET|PUT|POST|DELETE /dataset/{id}/aliases` — Alias management:
> - **GET**: Returns JSON array of alias strings
> - **POST**: Adds aliases to the existing set
> - **PUT**: Replaces all aliases with the provided set
> - **DELETE**: Removes all aliases
>
> Aliases must be alphanumeric only. Invalid aliases return 400.

> **API-3.10** `GET /dataset/{id}/export` — Exports the full dataset. Content negotiation determines format:
> - `Accept: application/json` → JSON array, streamed, flushed every 10,000 rows
> - `Accept: text/csv` → CSV file, multi-threaded writing, flushed every 10,000 rows
>
> Sets `Content-Disposition` header with filename. Returns 404 if not found, 424 if status is CREATED.

> **API-3.11** `GET /dataset/{id}/swagger` — Returns a dynamically generated OpenAPI specification for the dataset, with query parameters derived from column names and server configuration.

> **API-3.12** `GET /dataset/{id}/html` — Returns the web GUI for browsing the dataset. Supports `?embed` parameter for minimal iframe-friendly rendering. See [09-ui-ux.md](09-ui-ux.md).

> **API-3.13** `GET /` — Returns 404 (no root resource).

## API-4 Content Negotiation

> **API-4.01** The `Accept` header determines the response format. JSON is the default.

> **API-4.02** The `format` query parameter overrides the `Accept` header when present.

> **API-4.03** `GET /dataset/{id}` with `Accept: text/html` responds with a **303 See Other** redirect to `/dataset/{id}/html`.

## API-5 Response Envelope

> **API-5.01** Query responses use this envelope structure:
> ```json
> {
>     "results": [ { "col1": "val1", "col2": "val2" }, ... ],
>     "limit": 100,
>     "offset": 0,
>     "resultCount": 1500,
>     "queryTime": 42,
>     "prev": null,
>     "next": "https://example.com/dataset/{id}?_limit=100&_offset=100&col=val"
> }
> ```
> - `results`: Array of JSONB row objects
> - `limit`: Applied limit value
> - `offset`: Applied offset value
> - `resultCount`: Total matching rows (from window function)
> - `queryTime`: Execution time in milliseconds
> - `prev`/`next`: Pagination URLs (null if not applicable)

## API-6 Error Response Conventions

> **API-6.01** Success statuses:
> - **200 OK** — Successful query, list, or status request
> - **202 Accepted** — CSV upload accepted for async processing
> - **204 No Content** — Successful alias update or dataset deletion with no response body

> **API-6.02** Client error statuses:
> - **400 Bad Request** — Invalid parameters, empty filter values, non-numeric limit/offset, invalid alias format
> - **404 Not Found** — Dataset or alias not found
> - **415 Unsupported Media Type** — Non-CSV content type for upload endpoints

> **API-6.03** Server-side constraint statuses:
> - **423 Locked** — DELETE attempted on a dataset that is not in AVAILABLE or ERROR state
> - **424 Failed Dependency** — Query or export attempted on a dataset in CREATED state (no data yet)
> - **429 Too Many Requests** — Rate limit exceeded (includes `Retry-After` header for sliding window type)

> **API-6.04** Server error statuses:
> - **500 Internal Server Error** — Unexpected server-side failure
> - **503 Service Unavailable** — Query timeout exceeded (PostgreSQL SQL state `57014`)

## API-7 JSONP Support

> **API-7.01** [REMOVED] JSONP support has been removed. Cross-origin access is now provided via CORS headers. See [SEC-6.01](07-security.md#sec-6-cors).

## API-8 Server Header

> **API-8.01** All responses include a `Server` header with value `RowStore` (configured via `server.server-header` in Spring Boot).

## API-9 Per-Dataset Swagger

> **API-9.01** Each dataset exposes a dynamically generated OpenAPI specification at `/dataset/{id}/swagger`. The spec includes query parameters derived from the dataset's column names and reflects the server's regex query configuration.

## Known Limitations

- No PATCH support for partial dataset updates
- No bulk operations endpoint
- No authentication on any endpoint (see [07-security.md](07-security.md))
- No versioning in URL path (no `/v1/` prefix)
- `_sort` parameter is recognized but not implemented

## References

- [ETL Pipeline](04-etl-pipeline.md#etl-1-pipeline-overview) — CSV processing triggered by POST/PUT
- [Query Processing](05-querying.md#query-4-regex-modes) — Query filter mechanics and regex modes
- [Configuration](06-configuration.md#cfg-3-application-options) — `baseurl`, `querymaxlimit` options
- [Security](07-security.md#sec-5-rate-limiting) — Rate limiting and input validation
- [Web GUI](09-ui-ux.md#uix-2-display-modes) — Web GUI endpoint
- [Glossary](12-glossary.md#glo-1-terms) — Export, Swagger/OpenAPI, CORS
- Source: `DatasetController.java`, `DatasetsController.java`, `AliasController.java`, `StatusController.java`, `ExportController.java`, `WebGuiController.java`, `SwaggerController.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: JSONP removed, CORS added, DELETE returns 204, source references updated |
