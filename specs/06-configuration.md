# Configuration

## Purpose

Documents all RowStore configuration options, their defaults, environment variables, CLI arguments, and provides an annotated example.

## Scope

Covers the JSON configuration file format, all application and database options, rate limit settings, CLI arguments, and environment variables. For how specific options affect behavior, see the referenced spec files.

## CFG-1 File Format

> **CFG-1.01** Configuration is specified as a JSON file. The file must be valid JSON with a single root object.

## CFG-2 Resolution Order

> **CFG-2.01** The configuration file is resolved in this order:
> 1. `--rowstore.config.uri=<URI>` Spring Boot property (command line)
> 2. `ROWSTORE_CONFIG_URI` environment variable
> 3. `rowstore.json` from the classpath (fallback)

> **CFG-2.02** The URI supports these schemes: `file://`, `http://`, `https://`. Relative file paths are resolved against the working directory.

## CFG-3 Application Options

> **CFG-3.01** `baseurl` (String, **required**) — The public-facing base URL of the RowStore instance. Used in response bodies for constructing resource URLs, pagination links, and JSON-LD `@id` values. Example: `"https://rowstore.example.com"`.

> **CFG-3.02** `regexpqueries` (String, default `"false"`) — Controls regex query support:
> - `"false"` or `"disabled"` — Exact match only
> - `"simple"` — Regex with `^` prefix required
> - `"true"` or `"full"` — Full regex with heuristic detection
>
> See [QUERY-4](05-querying.md#query-4-regex-modes) for details.

> **CFG-3.03** `maxetlprocesses` (int, default `5`) — Maximum number of concurrent ETL processing tasks. Controls the Semaphore permit count for virtual thread concurrency. See [ETL-2.02](04-etl-pipeline.md#etl-2-concurrency-mechanism).

> **CFG-3.04** `loglevel` (String, default `"info"`) — Log level for the application. Valid values: `DEBUG`, `INFO`, `WARN`, `ERROR` (case-insensitive).

> **CFG-3.05** `legacyparser` (boolean, default `false`) — When `true`, uses OpenCSV's `CSVParserBuilder` instead of `RFC4180ParserBuilder` for CSV parsing. See [ETL-4.03](04-etl-pipeline.md#etl-4-csv-processing).

> **CFG-3.06** `querytimeout` (int, default `-1`) — Query timeout in seconds. When > 0, applied via `setQueryTimeout()` on the JDBC statement. Timed-out queries return 503. See [QUERY-7.01](05-querying.md#query-7-query-timeout).

> **CFG-3.07** `querymaxlimit` (int, default `100`) — Maximum value allowed for the `_limit` query parameter. Client-requested limits exceeding this value are capped. See [QUERY-3.01](05-querying.md#query-3-special-parameters).

> **CFG-3.08** `exportpagesize` (int, default `100000`) — Number of rows fetched per internal database query during export operations. Controls memory usage for large exports.

> **CFG-3.09** `maxuploadsize` (long, default `104857600`) — Maximum CSV upload size in bytes (default 100 MB). Uploads exceeding this limit are rejected. See [SEC-4.09](07-security.md#sec-4-input-validation).

> **CFG-3.10** `cors.allowedorigins` (JSON array, default `["*"]`) — List of allowed origins for CORS. When set to `["*"]`, all origins are permitted. See [SEC-6.01](07-security.md#sec-6-cors).

## CFG-4 Database Configuration

> **CFG-4.01** The `database` object (**required**) configures the primary PostgreSQL connection:
>
> | Property | Type | Default | Description |
> |----------|------|---------|-------------|
> | `type` | String | `"postgresql"` | Database type |
> | `host` | String | — | PostgreSQL host (required) |
> | `port` | int | `5432` | PostgreSQL port |
> | `database` | String | — | Database name (required) |
> | `user` | String | — | Database user (required) |
> | `password` | String | — | Database password (required) |

> **CFG-4.02** `ssl` (boolean, default `false`) — Enables SSL for the PostgreSQL connection.

> **CFG-4.03** Connection pool settings (HikariCP):
> - `connectionPoolInit` (int, default `-1`) — Minimum idle connections (`minimumIdle`). `-1` uses HikariCP defaults.
> - `connectionPoolMax` (int, default `-1`) — Maximum pool size (`maximumPoolSize`). `-1` uses HikariCP defaults.

> **CFG-4.04** Timeout settings (all in seconds):
> - `socketTimeout` (int, default `0`) — Socket read timeout. `0` means no timeout.
> - `connectTimeout` (int, default `5`) — Connection establishment timeout.
> - `loginTimeout` (int, default `5`) — Authentication timeout.

## CFG-5 Read Replica Configuration

> **CFG-5.01** The `queryDatabase` object (optional) configures a separate PostgreSQL connection pool for read queries. It accepts the same properties as `database`. When not configured, all queries use the primary `database` connection. See [QUERY-8.01](05-querying.md#query-8-read-replica-routing).

## CFG-6 Rate Limit Configuration

> **CFG-6.01** `ratelimit.type` (String, default `"slidingwindow"`) — Rate limiting strategy:
> - `"slidingwindow"` — Guava Cache with TTL-based tracking, provides `Retry-After` header
> - `"average"` — Guava `RateLimiter` for steady-rate enforcement, no `Retry-After` header

> **CFG-6.02** `ratelimit.timerange` (int, default `-1`) — Time window in seconds for rate limit accounting. Must be > 0 for rate limiting to be enabled.

> **CFG-6.03** `ratelimit.global`, `ratelimit.dataset`, `ratelimit.clientip` (int, default `-1`) — Maximum requests per `timerange` for each scope:
> - `global` — Single limiter for all requests
> - `dataset` — One limiter per dataset URL path
> - `clientip` — One limiter per client IP (from `X-Forwarded-For` header)

> **CFG-6.04** Rate limiting is enabled when: `timerange > 0 AND (global > 0 OR dataset > 0)`. If these conditions are not met, the `RateLimitFilter` is not active. See [SEC-5](07-security.md#sec-5-rate-limiting).

## CFG-7 CLI Options

> **CFG-7.01** `--rowstore.config.uri=<URI>` — Configuration file URI (Spring Boot property). Example: `--rowstore.config.uri=file:///etc/rowstore/config.json`.

> **CFG-7.02** `--server.port=<PORT>` — HTTP listen port. Default: `8282`. Standard Spring Boot property.

> **CFG-7.03** [REMOVED] `-l, --log-level` — CLI log level override. Use Spring Boot properties or Log4j2 configuration instead.

> **CFG-7.04** [REMOVED] `--connector-params` — Jetty connector parameters. Jetty has been replaced by embedded Tomcat.

## CFG-8 Environment Variables

> **CFG-8.01** `ROWSTORE_CONFIG_URI` — Configuration file URI. Used when no `--rowstore.config.uri` property is provided.

> **CFG-8.02** [REMOVED] `ROWSTORE_CONNECTOR_PARAMS` — Jetty connector parameters. Jetty has been replaced by embedded Tomcat.

## CFG-9 Annotated Example

> **CFG-9.01** Complete configuration example:
> ```json
> {
>     "baseurl": "https://rowstore.example.com",
>     "regexpqueries": "full",
>     "maxetlprocesses": 3,
>     "loglevel": "info",
>     "legacyparser": false,
>     "querytimeout": 30,
>     "querymaxlimit": 500,
>     "exportpagesize": 50000,
>     "maxuploadsize": 209715200,
>     "cors": {
>         "allowedorigins": ["https://example.com", "https://app.example.com"]
>     },
>     "database": {
>         "type": "postgresql",
>         "host": "db-primary.example.com",
>         "port": 5432,
>         "database": "rowstore",
>         "user": "rowstore_app",
>         "password": "secret",
>         "ssl": true,
>         "connectionPoolInit": 5,
>         "connectionPoolMax": 20,
>         "socketTimeout": 0,
>         "connectTimeout": 5,
>         "loginTimeout": 5
>     },
>     "queryDatabase": {
>         "type": "postgresql",
>         "host": "db-replica.example.com",
>         "port": 5432,
>         "database": "rowstore",
>         "user": "rowstore_reader",
>         "password": "secret",
>         "ssl": true
>     },
>     "ratelimit": {
>         "type": "slidingwindow",
>         "timerange": 60,
>         "global": 1000,
>         "dataset": 100,
>         "clientip": 50
>     }
> }
> ```

## Known Limitations

- No hot-reload of configuration (requires restart)
- No configuration validation at startup beyond JSON parsing (missing required fields cause runtime errors)
- Database password is stored in plaintext (no vault or secrets manager integration)
- No support for environment variable substitution within the JSON config file

## References

- [Query Processing](05-querying.md#query-4-regex-modes) — `regexpqueries`, `querytimeout`, `querymaxlimit` behavior
- [ETL Pipeline](04-etl-pipeline.md#etl-2-concurrency-mechanism) — `maxetlprocesses`, `legacyparser` behavior
- [Security](07-security.md#sec-5-rate-limiting) — Rate limit behavior
- [Deployment](08-deployment.md#depl-1-spring-boot-standalone) — CLI usage and deployment
- [Glossary](12-glossary.md#glo-1-terms) — Read Replica, Rate Limit, CORS
- Source: `RowStoreConfig.java`, `RowStoreConfigLoader.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: CLI args changed, Jetty params removed, maxuploadsize and cors.allowedorigins added |
