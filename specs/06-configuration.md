# Configuration

## Purpose

Documents all RowStore configuration options, their defaults, environment variables, CLI arguments, and provides an annotated example.

## Scope

Covers the JSON configuration file format, all application and database options, rate limit settings, CLI arguments, and environment variables. For how specific options affect behavior, see the referenced spec files.

## CFG-1 File Format

> **CFG-1.01** Configuration is specified as a JSON file. The file must be valid JSON with a single root object.

> **CFG-1.02** The configuration is validated at startup and RowStore refuses to start if it is invalid. The first error log line names the offending key (e.g. `rowstore.json: missing required key 'database.host'`). Startup fails when:
> - a required key is missing: `baseurl`, `database`, `database.host`, `database.database`, `database.user`, `database.password` (and the same four keys in `queryDatabase` when that block is present); an empty password is allowed
> - `database`, `queryDatabase`, `ratelimit` or `cors` is present but not a JSON object, or `cors.allowedorigins` is present but not an array of strings
> - `trustedproxies` is present but not an array of strings, or one of its entries is not an IP address or CIDR range
> - a value cannot be read as the expected type (e.g. `baseurl` is not a string)
> - `querymaxlimit` or `maxuploadsize` is not greater than 0
>
> Unknown keys are ignored.

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

> **CFG-3.07** `querymaxlimit` (int, default `100`) — Maximum value allowed for the `_limit` query parameter. Client-requested limits exceeding this value are capped. Must be greater than 0. See [QUERY-3.01](05-querying.md#query-3-special-parameters).

> **CFG-3.08** [REMOVED] `exportpagesize` — Was never used by the export implementation. The key is ignored if present.

> **CFG-3.09** `maxuploadsize` (long, default `104857600`) — Maximum CSV upload size in bytes (default 100 MB). Uploads exceeding this limit are rejected. Must be greater than 0; there is no setting for unlimited uploads (RowStore 1.x had no limit), so set a large value instead. See [SEC-4.09](07-security.md#sec-4-input-validation).

> **CFG-3.10** `cors.allowedorigins` (JSON array, default `["*"]`) — List of allowed origins for CORS. When set to `["*"]`, all origins are permitted. See [SEC-6.01](07-security.md#sec-6-cors).

> **CFG-3.11** `trustedproxies` (JSON array of strings) — IP addresses (`"192.0.2.10"`, `"::1"`) and CIDR ranges (`"10.0.0.0/8"`, `"fd00::/8"`) of reverse proxies whose `X-Forwarded-For` header is trusted for determining the client address (per-client-IP rate limit). Only IP literals are accepted, no host names. When absent (default), the connecting peer is trusted as the reverse proxy, since RowStore must only be reachable through it ([SEC-9.01](07-security.md#sec-9-reverse-proxy-contract)), and the last `X-Forwarded-For` entry is the client address. A list is needed if requests pass through more than one proxy whose addresses are not resolved before RowStore (e.g. a load balancer in front of nginx without nginx's real IP module), or to accept `X-Forwarded-For` only from known proxies. An empty array `[]` trusts nobody: `X-Forwarded-For` is always ignored. See [SEC-5.05](07-security.md#sec-5-rate-limiting) and [SEC-5.06](07-security.md#sec-5-rate-limiting).

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

> **CFG-4.02** `ssl` (boolean, optional) — SSL for the PostgreSQL connection, with and without connection pool. `true` sets the driver's `sslmode=require`, `false` sets `sslmode=disable`. When `ssl` is not set, the pgjdbc default `sslmode=prefer` applies (SSL if the server offers it), as in RowStore 1.7.

> **CFG-4.03** Connection pool settings (HikariCP):
> - `connectionPoolInit` (int, default `-1`) — Minimum idle connections (`minimumIdle`). `-1` uses HikariCP defaults.
> - `connectionPoolMax` (int, default `-1`) — Maximum pool size (`maximumPoolSize`). `-1` uses HikariCP defaults.

> **CFG-4.04** Timeout settings (all in seconds):
> - `socketTimeout` (int, default `0`) — Socket read timeout. `0` means no timeout.
> - `connectTimeout` (int, default `5`) — Connection establishment timeout.
> - `loginTimeout` (int, default `5`) — Authentication timeout.
>
> Values >= 0 are passed to the PostgreSQL driver, with and without connection pool (`0` means no timeout, as in 1.x). With HikariCP, `connectTimeout` additionally sets the pool's `connectionTimeout` (maximum wait for a pooled connection).

## CFG-5 Read Replica Configuration

> **CFG-5.01** The `queryDatabase` object (optional) configures a separate PostgreSQL connection pool for read queries. It accepts the same properties as `database`. When not configured, all queries use the primary `database` connection. Connections to `queryDatabase` are read-only also in auto-commit mode (driver `readOnlyMode=always`), with and without connection pool. See [QUERY-8.01](05-querying.md#query-8-read-replica-routing). Replica queries of a dataset may be cancelled when a replace of the dataset is replayed, see [ETL-6.04](04-etl-pipeline.md#etl-6-append-vs-replace). If `queryDatabase` uses a separate database user, it should be granted read access with `ALTER DEFAULT PRIVILEGES` (for tables created by the RowStore user), since every new dataset creates a new data table; privileges granted on an existing data table are kept by replaces (ETL-6.02).

## CFG-6 Rate Limit Configuration

> **CFG-6.01** `ratelimit.type` (String, default `"slidingwindow"`) — Rate limiting strategy:
> - `"slidingwindow"` — Guava Cache with TTL-based tracking, provides `Retry-After` header
> - `"average"` — Guava `RateLimiter` for steady-rate enforcement, no `Retry-After` header

> **CFG-6.02** `ratelimit.timerange` (int, default `-1`) — Time window in seconds for rate limit accounting. Must be > 0 for rate limiting to be enabled.

> **CFG-6.03** `ratelimit.global`, `ratelimit.dataset`, `ratelimit.clientip` (int, default `-1`) — Maximum requests per `timerange` for each scope:
> - `global` — Single limiter for all requests
> - `dataset` — One limiter per dataset URL path
> - `clientip` — One limiter per client IP: the client address from `X-Forwarded-For` if the connecting peer is a trusted proxy (by default any peer), otherwise the address of the connecting peer ([CFG-3.11](#cfg-3-application-options), [SEC-5.05](07-security.md#sec-5-rate-limiting))

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
>     "maxuploadsize": 209715200,
>     "cors": {
>         "allowedorigins": ["https://example.com", "https://app.example.com"]
>     },
>     "trustedproxies": ["10.0.0.5", "fd00:1::/64"],
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
- Validation covers required keys, block types and positive limits only (CFG-1.02); a scalar value of the wrong type for an optional key (e.g. `"querytimeout": "abc"`) still falls back to the default
- Database password is stored in plaintext (no vault or secrets manager integration)
- No support for environment variable substitution within the JSON config file

## References

- [Query Processing](05-querying.md#query-4-regex-modes) — `regexpqueries`, `querytimeout`, `querymaxlimit` behavior
- [ETL Pipeline](04-etl-pipeline.md#etl-2-concurrency-mechanism) — `maxetlprocesses`, `legacyparser` behavior
- [Security](07-security.md#sec-5-rate-limiting) — Rate limit behavior, trusted proxies
- [Deployment](08-deployment.md#depl-1-spring-boot-standalone) — CLI usage and deployment
- [Glossary](12-glossary.md#glo-1-terms) — Read Replica, Rate Limit, CORS
- Source: `RowStoreConfig.java`, `RowStoreConfigLoader.java`, `TrustedProxyFilter.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: CLI args changed, Jetty params removed, maxuploadsize and cors.allowedorigins added |
| 2026-10-20 | Added CFG-1.02 (fail fast on invalid config), CFG-3.08 removed (exportpagesize unused); CFG-4.02, CFG-4.04, CFG-5.01 clarified for driver settings with HikariCP |
| 2026-10-20 | Added CFG-3.11 (`trustedproxies`); CFG-1.02 and CFG-6.03 updated: `X-Forwarded-For` only from trusted proxies |
| 2026-10-21 | CFG-4.02: sslmode prefer when `ssl` is not set (as in 1.7); CFG-3.09: no unlimited upload size |
| 2026-10-21 | CFG-3.11: without `trustedproxies`, the connecting peer is trusted and the last `X-Forwarded-For` entry is the client (was: private ranges); CFG-6.03 accordingly |
| 2026-10-20 | CFG-5.01: replica queries may be cancelled by replayed replaces |
