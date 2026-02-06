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

## QUERY-3 Special Parameters

> **QUERY-3.01** `_limit` — Maximum number of rows to return. Default: **100**. Maximum: configured via `querymaxlimit` (default 100). Zero and negative values are treated as the default. Non-numeric values return 400.

> **QUERY-3.02** `_offset` — Number of rows to skip. Default: **0**. Negative values are corrected to 0. Non-numeric values return 400.

> **QUERY-3.03** `_callback` — JSONP function name. When present, the JSON response is wrapped in a function call: `callbackName({...})`. Handled by `JSCallbackFilter`.

> **QUERY-3.04** `format` — Response format override. Takes precedence over the `Accept` header for content negotiation.

## QUERY-4 Regex Modes

> **QUERY-4.01** **`disabled`** (config: `regexpqueries: "disabled"` or `"false"`): All filters use exact match only. SQL operator: `data->>? = ?`.

> **QUERY-4.02** **`simple`** (config: `regexpqueries: "simple"`): Values prefixed with `^` are treated as regex patterns. SQL operator: `data->>? ~ ?`. Values without `^` use exact match.

> **QUERY-4.03** **`full`** (config: `regexpqueries: "full"` or `"true"`): Two mechanisms trigger regex matching:
> 1. **Heuristic detection**: Values containing any of `^ $ ( | [ * + { ? /` are treated as regex
> 2. **Explicit prefix**: Values starting with `~` are treated as regex (the `~` is stripped before use)
>
> Values that don't trigger either mechanism use exact match as an optimization.

> **QUERY-4.04** The heuristic detection checks for the presence of these characters: `` ^ $ ( | [ * + { ? / ``. This is implemented in `DatasetUtil.isRegExpString()`.

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

> **QUERY-6.01** Total result count is obtained via the `count(*) OVER()` PostgreSQL window function, which computes the count across the full result set while returning only the current page.

> **QUERY-6.02** The response envelope includes `prev` and `next` URLs when applicable. These URLs preserve the original query parameters and adjust `_limit` and `_offset`:
> - `next`: present when `offset + limit < resultCount`
> - `prev`: present when `offset > 0`

## QUERY-7 Query Timeout

> **QUERY-7.01** When `querytimeout` is configured (value > 0), the timeout is applied via `stmt.setQueryTimeout(seconds)`. If PostgreSQL cancels the query (SQL state `57014`), the API returns **503 Service Unavailable**.

## QUERY-8 Read Replica Routing

> **QUERY-8.01** Query connections are obtained via `getQueryConnection()`, which returns a connection from the `queryDatabase` pool if configured, otherwise from the primary `database` pool. This enables routing read traffic to a PostgreSQL read replica.

## QUERY-9 Input Validation

> **QUERY-9.01** Empty filter values (e.g., `?column=`) return **400 Bad Request**.

> **QUERY-9.02** Non-numeric `_limit` or `_offset` values return **400 Bad Request**.

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
- Source: `DatasetResource.java`, `PgDataset.java` (query method), `DatasetUtil.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
