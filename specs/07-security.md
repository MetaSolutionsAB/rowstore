# Security

## Purpose

Documents the security posture of RowStore: authentication, authorization, input validation, SQL injection prevention, rate limiting, CORS, and other security-relevant behaviors.

## Scope

Covers all security-related aspects of the current system. This is a cross-cutting concern that references mechanisms documented in other specs. For rate limit configuration, see [06-configuration.md](06-configuration.md). For input validation behavior in queries, see [05-querying.md](05-querying.md).

## SEC-1 Authentication

> **SEC-1.01** [REMOVED] (2026-10-20) The pass-through `ApiKeyFilter` stub was deleted: there is no application-level API key, authentication is done by the reverse proxy (SEC-9).

> **SEC-1.02** No authentication is enforced by the application. Authentication of the private API is the responsibility of the reverse proxy, see [SEC-9](#sec-9-reverse-proxy-contract).

## SEC-2 Authorization

> **SEC-2.01** There is no role-based access control (RBAC), no user management, and no access control. Any client can create, query, modify, and delete any dataset.

## SEC-3 SQL Injection Prevention

> **SEC-3.01** All data queries use **prepared statements** with parameterized values (`data->>? = ?` and `data->>? ~ ?`). Column names and filter values are bound as parameters, not concatenated.

> **SEC-3.02** Dynamic DDL statements (table creation, index creation) use `BaseConnection.escapeString()` for column names that are interpolated into SQL strings. The `BaseConnection` is obtained via `Connection.unwrap(BaseConnection.class)` through the HikariCP proxy. This PostgreSQL-specific escaping prevents injection in contexts where prepared statement parameters cannot be used (e.g., `CREATE INDEX` column references).

## SEC-4 Input Validation

> **SEC-4.01** **Dataset IDs**: Validated as UUID format. Non-UUID strings are checked against the aliases table. If neither matches, a 404 is returned.

> **SEC-4.02** **Aliases**: Must be alphanumeric only (validated via `StringUtils.isAlphanumeric()`). Aliases containing special characters (!, @, -, spaces, UUID-like strings) return **400 Bad Request**.

> **SEC-4.03** **Query parameters**: Validated against the dataset's column names. Unrecognized parameters (not matching a column name or special parameter) return **400 Bad Request**.

> **SEC-4.04** **CSV Content-Type**: Upload endpoints accept the content type `text/csv` (case-insensitive, any parameters) or no Content-Type header, as in 1.x. Other content types return **415 Unsupported Media Type**.

> **SEC-4.05** **Empty query values**: Filter parameters with empty values (e.g., `?column=`) return **400 Bad Request**.

> **SEC-4.06** **`_limit`/`_offset`**: Must be valid integers. Non-numeric values return **400 Bad Request**. Values are capped or corrected (limit capped at `querymaxlimit`, negative offset corrected to 0).

> **SEC-4.07** **Data table name validation**: Data table names are validated against the regex pattern `data_[a-f0-9]{32}` before use in any SQL operation. This prevents SQL injection through malicious table names stored in the database.

> **SEC-4.08** [REMOVED] (2026-10-23) **ReDoS protection**: regex filter values were validated by `DatasetUtil.isSafeRegex()`, which rejected patterns longer than 200 characters or matching supposedly dangerous constructs with 400. The check rejected common regular expressions (e.g. `^(B|C)(o|e)*`) and, in full mode, long literal values detected as regex (e.g. URLs) that 1.x accepted, while PostgreSQL's regex engine is not susceptible to catastrophic backtracking. The run time of queries is limited by `querytimeout`, see [QUERY-4.05](05-querying.md#query-4-regex-modes) (ROWS-153).

> **SEC-4.09** **Upload size limit**: CSV uploads are limited to `maxuploadsize` bytes (default 100 MB, configurable). Uploads exceeding this limit are rejected.

> **SEC-4.10** **Alias row locking**: Alias mutation operations (PUT, POST, DELETE) use `SELECT ... FOR UPDATE` on the dataset row to prevent concurrent modification and ensure consistency.

> **SEC-4.11** **Path parameters**: Requests whose request path (the raw URI before `?`) contains `;` are rejected with **400 Bad Request** by `PathParameterFilter`, the first filter in the chain, so this also covers Actuator and springdoc endpoints and happens before rate limiting. RowStore uses no path parameters (neither did 1.x), but Tomcat and Spring ignore `;...` segments when routing: without this check `/v3;x/api-docs` or `/status;x?jvm` would bypass path rules of the reverse proxy (SEC-9.03), and `/dataset/{id};1`, `/dataset/{id};2`, … would each get their own per-dataset rate limit bucket. An encoded semicolon (`%3B`) is not a delimiter and is part of the path segment (e.g. an unknown dataset ID, 404).

## SEC-5 Rate Limiting

> **SEC-5.01** **Sliding window** (`type: "slidingwindow"`): Uses a Guava `Cache` with `expireAfterWrite` set to the `timerange`. Each request adds a timestamped entry. When the count within the window exceeds the limit, requests are rejected with **429 Too Many Requests** and a `Retry-After` header indicating when the oldest entry expires (oldest entry time + timerange + 1ms).

> **SEC-5.02** **Average** (`type: "average"`): Uses Guava's `RateLimiter.create()` with permits per second calculated as `limit / timerange`. Uses non-blocking `tryAcquire()`. When acquisition fails, returns **429 Too Many Requests** without a `Retry-After` header (returns -1).

> **SEC-5.03** Rate limiting operates at three scopes:
> - **Global**: A single rate limiter shared by all requests
> - **Per-dataset**: One rate limiter per dataset URL path (keyed by request path)
> - **Per-client IP**: One rate limiter per client IP address (the request's remote address, see SEC-5.05)
>
> Each scope has an independent limit. A request is rejected if it exceeds any active scope's limit.

> **SEC-5.04** Rate limiting applies to **GET and HEAD** requests only. POST, PUT, and DELETE are not rate limited. The `/status` endpoint is **exempt** from rate limiting. Rate limiter caches have a maximum size of **32,768 entries**.

> **SEC-5.05** **Client address**: The per-client-IP key is the request's remote address (`getRemoteAddr()`). `TrustedProxyFilter` replaces it with an address from `X-Forwarded-For` only if the connecting peer is a trusted proxy ([CFG-3.11](06-configuration.md#cfg-3-application-options)). Without `trustedproxies`, the connecting peer is trusted and the last entry of the header (all header lines, comma-separated) is the client address (SEC-5.06). With `trustedproxies`, the header is walked from right to left, skipping addresses of trusted proxies; the first untrusted address is the client address. If all entries are trusted proxies, the leftmost entry is used. Entries left of the client address can be set freely by the client and are ignored, so a client cannot choose its own rate limit key. With `trustedproxies`, entries are parsed as IP literals only; host names are never resolved. Entries with a port (`192.0.2.1:4711`) or an IPv6 zone ID (`fe80::1%eth0`) are not IP literals and count as untrusted, so proxies must write bare addresses. The zone ID of the connecting peer itself (a proxy connecting over link-local IPv6, e.g. `fe80::1%2`) comes from the local socket and is ignored when matching it against `trustedproxies`. If a peer that is not a trusted proxy sends `X-Forwarded-For`, a warning naming the peer and `trustedproxies` is logged at most once per hour per peer address (only with a non-empty `trustedproxies` list). The walk is the algorithm of Tomcat's `RemoteIpValve`, which is not used because its proxy lists are regular expressions, not CIDR ranges. Other forwarding headers (`X-Forwarded-Proto`, `X-Forwarded-Host`, `X-Forwarded-Port`, `Forwarded`) are not evaluated (`server.forward-headers-strategy=none`); all URLs in responses are built from `baseurl`.

> **SEC-5.06** **Trusted proxy default**: Without `trustedproxies`, the connecting peer is trusted as the reverse proxy, since RowStore must only be reachable through it (SEC-9.01), and the last `X-Forwarded-For` entry, which the proxy has appended (e.g. nginx `$proxy_add_x_forwarded_for`) or set (`$remote_addr`), is the client address. Entries further left are ignored, also if they are private addresses, so neither clients on the internet nor clients from private networks can choose their rate limit key. A reverse proxy on any address, including a public one, works without configuration. If RowStore is reachable without the proxy, which violates SEC-9.01, clients can set the last entry themselves, as in 1.x. An empty array trusts nobody: `X-Forwarded-For` is always ignored and the per-client-IP limit applies to the address of the connecting peer. An explicit list trusts only the listed peers and skips listed addresses from the right (SEC-5.05); it should then contain only reverse proxy addresses, since clients connecting from a listed range could choose their key. (1.x used any `X-Forwarded-For` value verbatim as the key.)

## SEC-6 CORS

> **SEC-6.01** Cross-Origin Resource Sharing (CORS) is implemented via Spring's `CorsFilter`. Allowed origins are configurable via `cors.allowedorigins` in `rowstore.json` (default: `["*"]`, allowing all origins). The filter adds `Access-Control-Allow-Origin`, `Access-Control-Allow-Methods`, and `Access-Control-Allow-Headers` headers, and handles preflight `OPTIONS` requests. It runs before `RateLimitFilter`, so `429` responses also carry the CORS headers and expose `Retry-After`.

## SEC-7 JSONP

> **SEC-7.01** [REMOVED] JSONP support has been removed. Cross-origin access is now provided via CORS headers (see [SEC-6.01](#sec-6-cors)).

## SEC-8 Temp File Handling

> **SEC-8.01** Uploaded CSV files are written to temp files marked with `deleteOnExit()` as a JVM shutdown safety net. Files are also explicitly deleted on early failure (before ETL queuing) and after successful processing.

## SEC-9 Reverse Proxy Contract

> **SEC-9.01** RowStore must always run behind a reverse proxy and must never be reachable directly by untrusted clients. The application has no authentication of its own (SEC-1.02); the security of a deployment depends on the reverse proxy fulfilling SEC-9.02 to SEC-9.04.

> **SEC-9.02** The reverse proxy authenticates all requests with methods other than `GET` and `HEAD` (the private API: creating, replacing, appending to and deleting datasets, managing aliases). `GET` and `HEAD` requests form the public read API.

> **SEC-9.03** The reverse proxy denies public access to these paths, which RowStore serves on the application port (8282) in 2.0, so blocking them at the proxy is the only protection (a separate management port is planned for 2.1, ROWS-108):
> - `/actuator` and `/actuator/**` (exposed endpoints: `health`, `info`, `metrics`)
> - `/swagger-ui.html` and `/swagger-ui/**` (springdoc Swagger UI)
> - `/v3/api-docs`, `/v3/api-docs/**` and `/v3/api-docs.yaml` (springdoc OpenAPI description)
> - `/status?jvm`, i.e. `/status` with a `jvm` query parameter in any form (JVM memory and processor details); `/status` without parameters is public
>
> Path rules must not be bypassable by path parameters: Tomcat and Spring ignore `;...` segments when routing (e.g. `/v3;x/api-docs` would serve the OpenAPI description and `/status;x?jvm` the JVM details). RowStore rejects such requests itself ([SEC-4.11](#sec-4-input-validation)); the proxy should still reject request paths containing `;` as defence in depth.

> **SEC-9.04** The reverse proxy is the only component that sets `X-Forwarded-For` towards RowStore: it appends the client address or replaces the header with it. If `trustedproxies` is set, the proxy's address must be listed ([CFG-3.11](06-configuration.md#cfg-3-application-options), [SEC-5.05](#sec-5-rate-limiting)), otherwise the per-client-IP rate limit is keyed on the proxy address.

> **SEC-9.05** POST requests can be tunnelled to another method with the query parameter `method` or the header `X-HTTP-Method-Override` ([API-4.06](03-api.md#api-4-content-negotiation)), e.g. `POST /dataset/{id}?method=DELETE` deletes the dataset. Since the proxy authenticates POST like every method other than GET and HEAD (SEC-9.02), this does not open the private API. A proxy that authorizes methods differently (e.g. allows POST to more users than DELETE) must reject or strip the `method` parameter and the `X-HTTP-Method-Override` header of POST requests. GET and HEAD requests are never tunnelled.

## Known Limitations

- No application-level authentication or authorization; the reverse proxy is responsible (SEC-9)
- No HTTPS termination (must be handled by a reverse proxy)
- No CSRF protection

## References

- [REST API](03-api.md#api-6-error-response-conventions) — Error status codes
- [Query Processing](05-querying.md#query-9-input-validation) — Input validation for query parameters
- [Configuration](06-configuration.md#cfg-6-rate-limit-configuration) — Rate limit configuration options
- [Glossary](12-glossary.md#glo-1-terms) — Rate Limit (Average), Rate Limit (Sliding Window), CORS
- [README Security section](../README.md#security) — Reverse proxy requirements and nginx example
- Source: `PathParameterFilter.java`, `ParameterParsingFilter.java`, `TunnelFilter.java`, `RateLimitFilter.java`, `TrustedProxyFilter.java`, `CorsConfig.java`, `DatasetController.java`, `AliasController.java`, `PgDataset.java`, `DatasetUtil.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: CORS implemented, JSONP removed, new validations (table name, ReDoS, upload size, alias locking) |
| 2026-10-20 | Added SEC-5.05 and SEC-5.06 (client address from `X-Forwarded-For` only via trusted proxies); SEC-5.03 updated |
| 2026-10-20 | SEC-1.01 removed (ApiKeyFilter stub deleted), SEC-1.02 reworded; added SEC-9 (reverse proxy contract) |
| 2026-10-20 | Added SEC-4.11 (path parameters rejected); SEC-9.03 refers to it |
| 2026-10-20 | SEC-6.01: CORS headers also on 429 responses |
| 2026-10-21 | SEC-5.06: without `trustedproxies`, the connecting peer is trusted and the last `X-Forwarded-For` entry is the client (was: private ranges); SEC-5.05 and SEC-9.04 accordingly |
| 2026-10-23 | SEC-4.04: uploads without Content-Type accepted (ROWS-156); SEC-4.08 removed (ROWS-153); added SEC-9.05 (method tunnel, ROWS-158) |
