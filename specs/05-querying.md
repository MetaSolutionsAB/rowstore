# Query Processing

## Purpose

Documents how RowStore processes query requests, including filter mechanics, regex modes, pagination, timeouts, and SQL construction.

## Scope

Covers the read path from query parameter parsing through SQL execution to response formatting. For the API endpoint definition, see [03-api.md](03-api.md). For the underlying data model, see [02-domain-model.md](02-domain-model.md).

## QUERY-1 Query Overview

> **QUERY-1.01** Queries are GET requests to `/dataset/{id}` (or `/dataset/{id}/json`) with column names as query parameters. Each parameter acts as a filter: only rows where the specified column matches the given value are returned. Multiple filters are combined with AND logic.

## QUERY-2 Filter Mechanics

> **QUERY-2.01** Query parameter names are matched **case-insensitively** against the dataset's column names (both are lowercased).

> **QUERY-2.02** Unrecognized query parameters (those not matching any column name or special parameter) cause a **400 Bad Request** response.

> **QUERY-2.03** Query parameters are parsed as in 1.x: names and values are trimmed (leading and trailing characters up to U+0020, e.g. spaces sent as `%20` or `+`), parameters whose name is empty after trimming are skipped, and of a repeated parameter the last value is used. Consequently, `?Name=Bo%20` matches `Bo`, and values with leading or trailing spaces cannot be matched exactly. A filter value that is empty only after trimming (e.g. `?Name=%20`, `?Name=+`) matches the rows whose cell is empty, as in 1.x. A filter value that is empty as sent (`?Name=`, or `?Name` without `=`) returns 400; 1.x returned 400 for `?Name=` as well, but matched empty cells for `?Name`. `_limit` and `_offset` with an empty or blank value (e.g. `?_limit=%20`, `?_offset=+`, `?_offset`) return 400, as in 1.x (QUERY-3.01, QUERY-3.02). Other parameters with a value that is empty as sent are accepted, unlike in 1.x, whose parser stopped at such a parameter and returned 400: `?_callback=` and `?_sort=` are ignored, and of `?Name=&Name=McLoud` the last value is used (200).

## QUERY-3 Special Parameters

> **QUERY-3.01** `_limit` — Maximum number of rows to return. Default: **100**. Maximum: configured via `querymaxlimit` (default 100). Zero, negative and values above `querymaxlimit` are treated as the default. Non-numeric values return 400.

> **QUERY-3.02** `_offset` — Number of rows to skip. Default: **0**. Negative values are corrected to 0. Non-numeric values return 400.

> **QUERY-3.03** [REMOVED] `_callback` — JSONP function name. JSONP support has been removed; use CORS for cross-origin access.

> **QUERY-3.04** [REMOVED] `format` — Response format override — never implemented, neither in 1.x nor in 2.0; see [API-4.02](03-api.md#api-4-content-negotiation).

> **QUERY-3.05** `_callback` — Reserved parameter kept for compatibility with 1.x clients. It is accepted and ignored: it is not treated as a column filter and the response is plain JSON (JSONP wrapping was removed in 2.0, see [QUERY-3.03](#query-3-special-parameters)). Like other non-pagination parameters, it is carried over to the `prev`/`next` URLs ([QUERY-6.02](#query-6-pagination)).

> **QUERY-3.06** `media`, `charset`, `encoding` and `language` — Parameters of the TunnelService of Restlet in 1.x, which consumed them for content negotiation ([API-4.05](03-api.md#api-4-content-negotiation)). They are not column filters and not carried over to the `prev`/`next` URLs, unless the dataset has a column of that name, in which case they filter it (in 1.x, such a filter was dropped silently and all rows were returned). On `/dataset/{id}`, a `media` value that is a media type or file extension still replaces the `Accept` header as well (e.g. `?media=html` redirects to `/dataset/{id}/html`); `/dataset/{id}/json` ignores the `Accept` header and can be used to filter such a column.

## QUERY-4 Regex Modes

> **QUERY-4.01** **`disabled`** (config: `regexpqueries: "disabled"` or `"false"`): All filters use exact match only. SQL operator: `data->>? = ?`.

> **QUERY-4.02** **`simple`** (config: `regexpqueries: "simple"`): Values prefixed with `^` are treated as regex patterns. SQL operator: `data->>? ~ ?`. Values without `^` use exact match.

> **QUERY-4.03** **`full`** (config: `regexpqueries: "full"` or `"true"`): Two mechanisms trigger regex matching:
> 1. **Heuristic detection**: Values containing any of `^ $ ( | [ * + { ? /` are treated as regex
> 2. **Explicit prefix**: Values starting with `~` are treated as regex (the `~` is stripped before use)
>
> Values that don't trigger either mechanism use exact match as an optimization.
>
> As in 1.x, the `~` prefix is not evaluated per value: from the first value with `~` on, all filters that follow in the (unspecified) iteration order of the query parameters are matched as regular expressions, also without `~` and without regex characters, and a leading `~` is stripped from every value. A query that combines a `~` value with other filters can therefore match those filters as regular expressions (ROWS-178).

> **QUERY-4.04** The heuristic detection checks for the presence of these characters: `` ^ $ ( | [ * + { ? / ``. This is implemented in `DatasetUtil.isRegExpString()`.

> **QUERY-4.05** Regular expressions are passed to PostgreSQL without further validation, as in 1.x; their length and structure are not restricted (see [SEC-4.08](07-security.md#sec-4-input-validation), removed). PostgreSQL's regex engine does not backtrack catastrophically like backtracking engines; the run time of a query is limited by `querytimeout` ([QUERY-7.01](#query-7-query-timeout)) only if it is configured; by default, there is no limit (ROWS-118). An invalid regular expression returns **400 Bad Request**.

## QUERY-5 SQL Construction

> **QUERY-5.01** Queries use prepared statements to prevent SQL injection:
> ```sql
> SELECT data, count(*) OVER() AS result_count
> FROM <data_table>
> WHERE data->>? = ?          -- exact match
>   AND data->>? ~ ?          -- regex match
> ORDER BY rownr
> LIMIT ? OFFSET ?
> ```
> Column names and values are bound as prepared statement parameters. The `count(*) OVER()` window function provides the total matching row count without a separate query.

## QUERY-6 Pagination

> **QUERY-6.01** Total result count is obtained via the `count(*) OVER()` PostgreSQL window function, which computes the count across the full result set while returning only the current page. The count is read from the returned rows, so an offset beyond the last match returns `resultCount` 0 (as in 1.x).

> **QUERY-6.02** The response envelope includes `prev` and `next` URLs when applicable. These URLs preserve the original query parameters and adjust `_limit` and `_offset`:
> - `next`: present when `offset + limit <= resultCount`, so a last page that is exactly full links to an empty page (e.g., `_limit=5` on 5 matching rows)
> - `prev`: present when `offset >= limit`, so a page with a smaller offset than its limit has no `prev` (e.g., `_offset=1&_limit=2`)
>
> These conditions are those of 1.x and are kept for compatibility.
>
> A filter value that is empty only after trimming ([QUERY-2.03](#query-2-filter-mechanics)) is carried over as a space (`col=+`), so that the links match empty cells as well; 1.x carried it over as `col=`, which returned 400. A value that is empty as sent (e.g. `_callback=`) is carried over empty.

## QUERY-7 Query Timeout

> **QUERY-7.01** When `querytimeout` is configured (value > 0), the timeout is applied via `stmt.setQueryTimeout(seconds)`. If PostgreSQL cancels the query (SQL state `57014`), the API returns **503 Service Unavailable**.

## QUERY-8 Read Replica Routing

> **QUERY-8.01** Query connections are obtained via `getQueryConnection()`, which returns a connection from the `queryDatabase` pool if configured, otherwise from the primary `database` pool. This enables routing read traffic to a PostgreSQL read replica.

## QUERY-9 Input Validation

> **QUERY-9.01** Empty filter values (e.g., `?column=`) return **400 Bad Request**.

> **QUERY-9.02** Non-numeric `_limit` or `_offset` values return **400 Bad Request**.

> **QUERY-9.03** The characters `` ^ | [ ] { } \ " < > ` `` are accepted unencoded in the query string, as in 1.x (Jetty), since browsers and tools such as curl send them unencoded, e.g. in `?name=^Ber` (`server.tomcat.relaxed-query-chars`). Non-ASCII characters must be percent-encoded (as browsers do); Tomcat rejects them unencoded with 400. In the request path, these characters are not relaxed (unencoded, Tomcat rejects them with 400). An encoded slash or backslash (`%2F`, `%5C`) in a dataset ID is passed through undecoded and remains part of the ID, so e.g. `/dataset/a%2Fb` and `/dataset/x%2F..%2Fstatus` return 404 for the unknown dataset, as in 1.x (Tomcat's default is 400).

> **QUERY-9.04** A query string that cannot be decoded, e.g. because of a malformed percent-encoding such as `?column=100%` or `?column=%zz`, or a percent-encoding that is not valid UTF-8 such as `?column=%C5` (ISO-8859-1), returns **400 Bad Request** (`ParameterParsingFilter`), so that a filter is never dropped silently and non-matching rows returned. In 1.x, such queries returned 0 rows or 500.

## Known Limitations

- No support for `_sort` parameter (present in code as a TODO, parameter is recognized but not implemented)
- No support for OR logic between filters (all filters are AND-combined)
- No support for null/missing value queries
- No full-text search (only exact match or regex)
- Regex matching uses PostgreSQL `~` operator (POSIX regex), not LIKE patterns

## References

- [REST API](03-api.md#api-5-response-envelope) — API endpoint definitions and response format
- [Domain Model](02-domain-model.md#dom-5-index-strategy) — Data table schema and indexes
- [Configuration](06-configuration.md#cfg-3-application-options) — `regexpqueries`, `querytimeout`, `querymaxlimit`
- [Glossary](12-glossary.md#glo-1-terms) — Query, Regex Mode, Pagination
- Source: `DatasetController.java`, `PgDataset.java` (query method), `DatasetUtil.java`, `ParameterParsingFilter.java`, `TunnelFilter.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: JSONP _callback removed, source references updated |
| 2026-10-20 | QUERY-3.01: values above `querymaxlimit` fall back to the default |
| 2026-10-21 | Added QUERY-3.05: `_callback` accepted and ignored instead of rejected with 400 (ROWS-147) |
| 2026-10-23 | Added QUERY-2.03 (trimming, ROWS-157), QUERY-3.06 (tunnel parameters, ROWS-158), QUERY-4.05 (no regex validation, ROWS-153), QUERY-9.03 (unencoded characters, ROWS-151), QUERY-9.04 (malformed encodings, ROWS-152) |
| 2026-10-23 | QUERY-3.04 removed, the `format` parameter was never implemented (ROWS-168) |
| 2026-11-05 | QUERY-3.06: `media` still selects the representation on `/dataset/{id}`; QUERY-4.05: run time only limited if `querytimeout` is configured |
| 2026-11-05 | QUERY-9.03: `%2F` and `%5C` in dataset IDs return 404, path characters are not relaxed (ROWS-151) |
| 2026-11-09 | QUERY-6.01: `resultCount` is 0 for an offset beyond the last match, as in 1.x (ROWS-170) |
| 2026-11-09 | QUERY-9.04: percent-encodings that are not UTF-8 return 400 with Tomcat 11 (ROWS-143) |
| 2026-11-09 | QUERY-6.02: conditions of `prev` and `next` as implemented in 1.x and 2.0; QUERY-4.03: a `~` value switches the following filters to regex matching (ROWS-177) |
| 2026-11-09 | QUERY-2.03: a value that is empty only after trimming matches empty cells, a value that is empty as sent returns 400 (ROWS-175) |
| 2026-11-09 | QUERY-2.03: empty `_callback`, `_sort` and overridden values are accepted (1.x: 400); QUERY-6.02: blank filter values are carried over to the page links as a space (ROWS-175) |
