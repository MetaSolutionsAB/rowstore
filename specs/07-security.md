# Security

## Purpose

Documents the security posture of RowStore: authentication, authorization, input validation, SQL injection prevention, rate limiting, CORS, and other security-relevant behaviors.

## Scope

Covers all security-related aspects of the current system. This is a cross-cutting concern that references mechanisms documented in other specs. For rate limit configuration, see [06-configuration.md](06-configuration.md). For input validation behavior in queries, see [05-querying.md](05-querying.md).

## SEC-1 Authentication

> **SEC-1.01** An `ApiKeyFilter` exists in the codebase but is a **stub implementation**. It passes all requests through without authentication (`implements jakarta.servlet.Filter`).

> **SEC-1.02** No authentication is currently enforced on any endpoint. All API endpoints are publicly accessible.

## SEC-2 Authorization

> **SEC-2.01** There is no role-based access control (RBAC), no user management, and no access control. Any client can create, query, modify, and delete any dataset.

## SEC-3 SQL Injection Prevention

> **SEC-3.01** All data queries use **prepared statements** with parameterized values (`data->>? = ?` and `data->>? ~ ?`). Column names and filter values are bound as parameters, not concatenated.

> **SEC-3.02** Dynamic DDL statements (table creation, index creation) use `BaseConnection.escapeString()` for column names that are interpolated into SQL strings. The `BaseConnection` is obtained via `Connection.unwrap(BaseConnection.class)` through the HikariCP proxy. This PostgreSQL-specific escaping prevents injection in contexts where prepared statement parameters cannot be used (e.g., `CREATE INDEX` column references).

## SEC-4 Input Validation

> **SEC-4.01** **Dataset IDs**: Validated as UUID format. Non-UUID strings are checked against the aliases table. If neither matches, a 404 is returned.

> **SEC-4.02** **Aliases**: Must be alphanumeric only (validated via `StringUtils.isAlphanumeric()`). Aliases containing special characters (!, @, -, spaces, UUID-like strings) return **400 Bad Request**.

> **SEC-4.03** **Query parameters**: Validated against the dataset's column names. Unrecognized parameters (not matching a column name or special parameter) return **400 Bad Request**.

> **SEC-4.04** **CSV Content-Type**: Upload endpoints enforce `text/csv` content type. Non-CSV content types return **415 Unsupported Media Type**.

> **SEC-4.05** **Empty query values**: Filter parameters with empty values (e.g., `?column=`) return **400 Bad Request**.

> **SEC-4.06** **`_limit`/`_offset`**: Must be valid integers. Non-numeric values return **400 Bad Request**. Values are capped or corrected (limit capped at `querymaxlimit`, negative offset corrected to 0).

> **SEC-4.07** **Data table name validation**: Data table names are validated against the regex pattern `data_[a-f0-9]{32}` before use in any SQL operation. This prevents SQL injection through malicious table names stored in the database.

> **SEC-4.08** **ReDoS protection**: Regex filter values are validated by `DatasetUtil.isSafeRegex()` before being passed to PostgreSQL. Patterns exceeding a maximum length or matching known dangerous patterns are rejected with **400 Bad Request**.

> **SEC-4.09** **Upload size limit**: CSV uploads are limited to `maxuploadsize` bytes (default 100 MB, configurable). Uploads exceeding this limit are rejected.

> **SEC-4.10** **Alias row locking**: Alias mutation operations (PUT, POST, DELETE) use `SELECT ... FOR UPDATE` on the dataset row to prevent concurrent modification and ensure consistency.

## SEC-5 Rate Limiting

> **SEC-5.01** **Sliding window** (`type: "slidingwindow"`): Uses a Guava `Cache` with `expireAfterWrite` set to the `timerange`. Each request adds a timestamped entry. When the count within the window exceeds the limit, requests are rejected with **429 Too Many Requests** and a `Retry-After` header indicating when the oldest entry expires (oldest entry time + timerange + 1ms).

> **SEC-5.02** **Average** (`type: "average"`): Uses Guava's `RateLimiter.create()` with permits per second calculated as `limit / timerange`. Uses non-blocking `tryAcquire()`. When acquisition fails, returns **429 Too Many Requests** without a `Retry-After` header (returns -1).

> **SEC-5.03** Rate limiting operates at three scopes:
> - **Global**: A single rate limiter shared by all requests
> - **Per-dataset**: One rate limiter per dataset URL path (keyed by request path)
> - **Per-client IP**: One rate limiter per client IP address (extracted from the `X-Forwarded-For` header)
>
> Each scope has an independent limit. A request is rejected if it exceeds any active scope's limit.

> **SEC-5.04** Rate limiting applies to **GET and HEAD** requests only. POST, PUT, and DELETE are not rate limited. The `/status` endpoint is **exempt** from rate limiting. Rate limiter caches have a maximum size of **32,768 entries**.

## SEC-6 CORS

> **SEC-6.01** Cross-Origin Resource Sharing (CORS) is implemented via Spring's `CorsFilter`. Allowed origins are configurable via `cors.allowedorigins` in `rowstore.json` (default: `["*"]`, allowing all origins). The filter adds `Access-Control-Allow-Origin`, `Access-Control-Allow-Methods`, and `Access-Control-Allow-Headers` headers, and handles preflight `OPTIONS` requests.

## SEC-7 JSONP

> **SEC-7.01** [REMOVED] JSONP support has been removed. Cross-origin access is now provided via CORS headers (see [SEC-6.01](#sec-6-cors)).

## SEC-8 Temp File Handling

> **SEC-8.01** Uploaded CSV files are written to temp files marked with `deleteOnExit()` as a JVM shutdown safety net. Files are also explicitly deleted on early failure (before ETL queuing) and after successful processing.

## Known Limitations

- No authentication or authorization (all endpoints are public)
- No HTTPS termination (must be handled by a reverse proxy)
- No CSRF protection

## References

- [REST API](03-api.md#api-6-error-response-conventions) — Error status codes
- [Query Processing](05-querying.md#query-9-input-validation) — Input validation for query parameters
- [Configuration](06-configuration.md#cfg-6-rate-limit-configuration) — Rate limit configuration options
- [Glossary](12-glossary.md#glo-1-terms) — Rate Limit (Average), Rate Limit (Sliding Window), CORS
- Source: `ApiKeyFilter.java`, `RateLimitFilter.java`, `CorsConfig.java`, `DatasetController.java`, `AliasController.java`, `PgDataset.java`, `DatasetUtil.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: CORS implemented, JSONP removed, new validations (table name, ReDoS, upload size, alias locking) |
