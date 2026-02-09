# RowStore Specification Suite

This specification suite is the canonical reference for the RowStore system. It documents the **current state** of the software (version 2.0-SNAPSHOT, branch `develop`) and serves all four user personas: data publishers, API consumers, operators/admins, and embedded viewers.

The existing framework-agnostic test specifications in [`specs/tests/`](tests/README.md) (121 test cases across 9 files) are preserved alongside these specs.

## Reading Paths by Persona

| Persona | Start Here | Then Read |
|---------|------------|-----------|
| **Data Publisher** | [01-system-architecture.md](01-system-architecture.md) | [03-api.md](03-api.md) (POST/PUT), [04-etl-pipeline.md](04-etl-pipeline.md), [06-configuration.md](06-configuration.md) (if self-hosting) |
| **API Consumer** | [03-api.md](03-api.md) | [05-querying.md](05-querying.md), [12-glossary.md](12-glossary.md) |
| **Operator/Admin** | [01-system-architecture.md](01-system-architecture.md) | [06-configuration.md](06-configuration.md), [08-deployment.md](08-deployment.md), [07-security.md](07-security.md) |
| **Embedded Viewer** | [09-ui-ux.md](09-ui-ux.md) | [03-api.md](03-api.md) (query params) |

## Specification Files

| File | Prefix | Description |
|------|--------|-------------|
| [01-system-architecture.md](01-system-architecture.md) | `ARCH` | What RowStore is, technology stack, module and component architecture |
| [02-domain-model.md](02-domain-model.md) | `DOM` | Conceptual model, entity relationships, database schema, indexes |
| [03-api.md](03-api.md) | `API` | REST API design, endpoint catalog, error conventions |
| [04-etl-pipeline.md](04-etl-pipeline.md) | `ETL` | CSV upload processing: virtual threads, parsing, batch inserts, indexing |
| [05-querying.md](05-querying.md) | `QUERY` | Query filters, regex modes, pagination, timeouts |
| [06-configuration.md](06-configuration.md) | `CFG` | All config options, defaults, env vars, CLI args, examples |
| [07-security.md](07-security.md) | `SEC` | Auth state, input validation, rate limiting, CORS, SQL safety |
| [08-deployment.md](08-deployment.md) | `DEPL` | Spring Boot standalone, Docker, CI/CD, monitoring, logging |
| [09-ui-ux.md](09-ui-ux.md) | `UIX` | Web GUI features and display modes |
| [10-testing.md](10-testing.md) | `TEST` | Test strategy, infrastructure, coverage matrix |
| [11-user-stories.md](11-user-stories.md) | `USR` | Scenario-based stories for all 4 personas |
| [12-glossary.md](12-glossary.md) | `GLO` | Domain terminology definitions |

## Requirement ID Scheme

Every substantive requirement has a permanent ID in the format:

```
{PREFIX}-{section}.{number}
```

- **PREFIX**: Identifies the spec domain (see table above)
- **section**: Groups related requirements (integer: 1, 2, 3...)
- **number**: Zero-padded requirement number within section (01, 02, 03...)

**Examples**: `ARCH-1.01`, `API-3.02`, `SEC-4.05`, `QUERY-4.03`

### How IDs appear in spec files

```markdown
## ARCH-1 What RowStore Is
> ARCH-1.01 RowStore is a CSV-to-JSON pipeline with a REST query interface.
> ARCH-1.02 Data is stored in PostgreSQL using JSONB columns.
```

### Referencing from code or issues

```java
// See QUERY-4.03 for regex heuristic detection rules
```
```
Fixes #42 — behavior now matches API-4.01
```

### Governance Rules

1. Existing IDs are **permanent** — never change, never reuse
2. New requirements within a section: use the next available number (e.g., after SEC-4.06, add SEC-4.07)
3. New sections: use the next available section number (e.g., after ARCH-6, add ARCH-7)
4. Removed requirements keep their ID with `[REMOVED]` marker — the number is never reassigned
5. The Purpose, Scope, Known Limitations, References, and Change Log sections do NOT get spec IDs — only substantive content sections do

## Spec File Template

Every spec file follows this structure:

```markdown
# [Title]

## Purpose
What this spec covers and why.

## Scope
What's included/excluded. Links to related specs.

## [Content Sections — each with PREFIX-N heading and PREFIX-N.NN requirement IDs]

## Known Limitations
Current limitations relevant to this area.

## References
- Links to related spec files
- Links to key source files

## Change Log
| Date | Description |
|------|-------------|
| YYYY-MM-DD | Initial version |
```

## Full Requirement ID Registry

### ARCH — System Architecture

| ID | Description |
|----|-------------|
| ARCH-1.01 | CSV-to-JSON pipeline with REST query interface |
| ARCH-1.02 | PostgreSQL JSONB storage backend |
| ARCH-2.01 | Upload CSV datasets |
| ARCH-2.02 | Query via REST with column filters |
| ARCH-2.03 | Export as JSON or CSV |
| ARCH-2.04 | Web GUI for browsing |
| ARCH-2.05 | Rate limiting |
| ARCH-3.01 | Core: Java 25, Spring Boot 3.5.10, PostgreSQL, Tomcat (embedded) |
| ARCH-3.02 | Libraries: OpenCSV, Guava, Log4j2, juniversalchardet, ICU4J |
| ARCH-4.01 | Single Maven module — executable JAR |
| ARCH-4.02 | [REMOVED] standalone/jetty — replaced by Spring Boot embedded server |
| ARCH-4.03 | [REMOVED] standalone/common — replaced by Spring Boot embedded server |
| ARCH-5.01 | Filter chain: CorsFilter → ApiKeyFilter → RateLimitFilter → DispatcherServlet → Controllers |
| ARCH-5.02 | Read path: GET → controller → PgDataset.query → PostgreSQL |
| ARCH-5.03 | Write path: POST CSV → temp file → EtlProcessor.submit → virtual thread → PgDataset.populate |
| ARCH-6.01 | Why JSONB (schemaless, per-column indexing) |
| ARCH-6.02 | Why virtual-thread ETL (immediate dispatch, Semaphore concurrency control) |
| ARCH-6.03 | Why Spring Boot (industry standard, HikariCP, Flyway, Actuator, virtual threads) |

### DOM — Domain Model

| ID | Description |
|----|-------------|
| DOM-1.01 | Dataset entity: UUID, status, created, columns, row count |
| DOM-1.02 | Alias entity: alphanumeric string, belongs to Dataset |
| DOM-1.03 | Row entity: sequential rownr, JSONB data |
| DOM-1.04 | ETL Job: processing wrapper with temp file and queue position |
| DOM-2.01 | Dataset 1:N Alias |
| DOM-2.02 | Dataset 1:N Row |
| DOM-2.03 | Dataset has ETL Status |
| DOM-3.01 | Status lifecycle: CREATED→ACCEPTED_DATA→PROCESSING→AVAILABLE\|ERROR |
| DOM-4.01 | datasets table schema |
| DOM-4.02 | aliases table schema |
| DOM-4.03 | Per-dataset data table schema |
| DOM-5.01 | text_pattern_ops indexes per column |
| DOM-5.02 | MD5-hashed index names |
| DOM-5.03 | Fields > 256 chars not indexed |
| DOM-6.01 | Data table naming: data_ + UUID without hyphens |
| DOM-7.01 | JSON-LD output with @context and @id |

### API — REST API

| ID | Description |
|----|-------------|
| API-1.01 | RESTful resource-oriented design |
| API-1.02 | JSON default response format |
| API-1.03 | Content negotiation via Accept header and format param |
| API-1.04 | [REMOVED] JSONP via _callback param — replaced by CORS |
| API-2.01 | Base URL from config, used in response links |
| API-3.01 | GET /status — service status, exempt from rate limiting |
| API-3.02 | GET /datasets — list dataset UUIDs |
| API-3.03 | POST /datasets — create dataset from CSV (202) |
| API-3.04 | GET /dataset/{id} and /json — query with filters |
| API-3.05 | POST /dataset/{id} — append CSV data |
| API-3.06 | PUT /dataset/{id} — replace CSV data |
| API-3.07 | DELETE /dataset/{id} — delete dataset (204) |
| API-3.08 | GET /dataset/{id}/info — dataset metadata (JSON-LD) |
| API-3.09 | GET\|PUT\|POST\|DELETE /dataset/{id}/aliases — alias management |
| API-3.10 | GET /dataset/{id}/export — export JSON or CSV |
| API-3.11 | GET /dataset/{id}/swagger — dynamic OpenAPI spec |
| API-3.12 | GET /dataset/{id}/html — web GUI |
| API-3.13 | GET / — 404 fallback |
| API-4.01 | Accept header determines response format |
| API-4.02 | format param overrides Accept header |
| API-4.03 | Accept: text/html redirects 303 to /html |
| API-5.01 | Response envelope: results, limit, offset, resultCount, queryTime, prev, next |
| API-6.01 | Success: 200, 202, 204 |
| API-6.02 | Client errors: 400, 404, 415 |
| API-6.03 | Constraints: 423, 424, 429 |
| API-6.04 | Server errors: 500, 503 |
| API-7.01 | [REMOVED] _callback wraps JSON in function call — JSONP removed |
| API-8.01 | Server header: RowStore |
| API-9.01 | Per-dataset OpenAPI spec from column names |

### ETL — ETL Pipeline

| ID | Description |
|----|-------------|
| ETL-1.01 | Pipeline: upload → temp file → virtual thread → parse → batch insert → index |
| ETL-2.01 | Virtual threads with Semaphore-based concurrency limiting |
| ETL-2.02 | Max concurrent from maxetlprocesses (default 5) |
| ETL-3.01 | Status transitions: CREATED→ACCEPTED_DATA→PROCESSING→AVAILABLE\|ERROR |
| ETL-3.02 | Error handling: status set to ERROR, temp file deleted |
| ETL-4.01 | Charset detection: juniversalchardet → ICU4J → UTF-8 |
| ETL-4.02 | Separator detection: semicolon if consistent in first 2 lines, else comma |
| ETL-4.03 | Parser: RFC4180Parser (default) or CSVParser (legacy) |
| ETL-4.04 | Column names: lowercase, trimmed, empty skipped |
| ETL-5.01 | Batch insert every 200 rows, rollback on error |
| ETL-6.01 | POST = append (validates column match) |
| ETL-6.02 | PUT = truncate + reload |
| ETL-7.01 | text_pattern_ops index per column, escapeString for DDL |
| ETL-7.02 | Skip index for fields > 256 chars |
| ETL-8.01 | Concurrency: Semaphore + AtomicInteger, busy-wait if PROCESSING |
| ETL-9.01 | Temp file: deleteOnExit + explicit deletion |

### QUERY — Query Processing

| ID | Description |
|----|-------------|
| QUERY-1.01 | Column filters via query params with regex and pagination |
| QUERY-2.01 | Case-insensitive column name matching |
| QUERY-2.02 | Unrecognized params → 400 |
| QUERY-3.01 | _limit: default 100, max from querymaxlimit |
| QUERY-3.02 | _offset: default 0, negatives corrected to 0 |
| QUERY-3.03 | [REMOVED] _callback: JSONP function name — JSONP removed |
| QUERY-3.04 | format: response format override |
| QUERY-4.01 | disabled: exact match only (=) |
| QUERY-4.02 | simple: ^-prefixed values become regex (~) |
| QUERY-4.03 | full: heuristic detection OR ~ prefix forces regex |
| QUERY-4.04 | Heuristic chars: ^ $ ( \| [ * + { ? / |
| QUERY-5.01 | Prepared statements: data->>? = ? or data->>? ~ ? |
| QUERY-6.01 | count(*) OVER() window function for total count |
| QUERY-6.02 | Prev/next URLs with original query params |
| QUERY-7.01 | setQueryTimeout, SQL state 57014 → 503 |
| QUERY-8.01 | getQueryConnection() routes to queryDatabase if configured |
| QUERY-9.01 | Empty filter values → 400 |
| QUERY-9.02 | Non-numeric _limit/_offset → 400 |

### CFG — Configuration

| ID | Description |
|----|-------------|
| CFG-1.01 | JSON configuration file format |
| CFG-2.01 | Resolution: Spring property → env var → classpath |
| CFG-2.02 | Schemes: file, http, https |
| CFG-3.01 | baseurl (String, required) |
| CFG-3.02 | regexpqueries (disabled/simple/full, default false) |
| CFG-3.03 | maxetlprocesses (int, default 5) |
| CFG-3.04 | loglevel (String, default info) |
| CFG-3.05 | legacyparser (boolean, default false) |
| CFG-3.06 | querytimeout (int seconds, default -1) |
| CFG-3.07 | querymaxlimit (int, default 100) |
| CFG-3.08 | exportpagesize (int, default 100000) |
| CFG-3.09 | maxuploadsize (long bytes, default 104857600) |
| CFG-3.10 | cors.allowedorigins (JSON array, default ["*"]) |
| CFG-4.01 | database object: type, host, port, database, user, password |
| CFG-4.02 | ssl (boolean, default false) |
| CFG-4.03 | connectionPoolInit, connectionPoolMax (default -1) |
| CFG-4.04 | socketTimeout (0), connectTimeout (5), loginTimeout (5) |
| CFG-5.01 | queryDatabase (optional, falls back to database) |
| CFG-6.01 | ratelimit.type (slidingwindow/average) |
| CFG-6.02 | ratelimit.timerange (seconds) |
| CFG-6.03 | ratelimit.global, .dataset, .clientip (requests per timerange) |
| CFG-6.04 | Enable: timerange > 0 AND (global > 0 OR dataset > 0) |
| CFG-7.01 | --rowstore.config.uri — config file URI (Spring property) |
| CFG-7.02 | --server.port — listen port (default 8282) |
| CFG-7.03 | [REMOVED] -l/--log-level — CLI log level override |
| CFG-7.04 | [REMOVED] --connector-params — Jetty connector params |
| CFG-8.01 | ROWSTORE_CONFIG_URI env var |
| CFG-8.02 | [REMOVED] ROWSTORE_CONNECTOR_PARAMS — Jetty connector params |
| CFG-9.01 | Annotated example config |

### SEC — Security

| ID | Description |
|----|-------------|
| SEC-1.01 | ApiKeyFilter is a stub — always passes through |
| SEC-1.02 | No authentication enforced |
| SEC-2.01 | No RBAC, no user management, no access control |
| SEC-3.01 | Prepared statements for all data queries |
| SEC-3.02 | escapeString() for dynamic DDL |
| SEC-4.01 | Dataset IDs: UUID validation |
| SEC-4.02 | Aliases: alphanumeric only → 400 |
| SEC-4.03 | Query params: validated against column names → 400 |
| SEC-4.04 | CSV Content-Type enforced → 415 |
| SEC-4.05 | Empty query values → 400 |
| SEC-4.06 | _limit/_offset integer validation → 400 |
| SEC-4.07 | Data table name validation: regex `data_[a-f0-9]{32}` |
| SEC-4.08 | ReDoS protection: isSafeRegex() validates length and pattern complexity |
| SEC-4.09 | Upload size limit: maxuploadsize (default 100 MB) |
| SEC-4.10 | Alias row locking: SELECT FOR UPDATE prevents concurrent modification |
| SEC-5.01 | Sliding window: Guava Cache, Retry-After header |
| SEC-5.02 | Average: Guava RateLimiter, no Retry-After |
| SEC-5.03 | Scopes: global, per-dataset, per-client-IP |
| SEC-5.04 | GET/HEAD only; /status exempt; cache max 32768 |
| SEC-6.01 | CORS: configurable allowed origins via cors.allowedorigins |
| SEC-7.01 | [REMOVED] JSONP via _callback param — replaced by CORS |
| SEC-8.01 | Temp files: deleteOnExit + explicit deletion |

### DEPL — Deployment

| ID | Description |
|----|-------------|
| DEPL-1.01 | Build: mvn -Dmaven.test.skip=true install |
| DEPL-1.02 | Run: java -jar rowstore-\<version\>.jar --rowstore.config.uri=file:///path/to/config.json |
| DEPL-1.03 | Spring Boot standard properties (--server.port, etc.) |
| DEPL-1.04 | [REMOVED] HTTP/2 and useForwardedForHeader — Jetty-specific |
| DEPL-2.01 | Docker image: metasolutions/rowstore |
| DEPL-2.02 | Dockerfile in the RowStore repository |
| DEPL-2.03 | Docker tags: version, major.minor, develop |
| DEPL-3.01 | Pipeline: build → test → deploy + Docker Hub |
| DEPL-3.02 | Branch strategy: master, develop, other |
| DEPL-3.03 | Artifacts: executable JAR + SHA256 + GPG |
| DEPL-4.01 | PostgreSQL 9.4+ for JSONB |
| DEPL-4.02 | Schema managed by Flyway (baselineOnMigrate) |
| DEPL-5.01 | GET /status: service, version, datasets, activeEtlProcesses |
| DEPL-5.02 | GET /status?jvm: memory, processors, heap |
| DEPL-5.03 | Spring Boot Actuator: /actuator/health, /actuator/metrics, /actuator/info |
| DEPL-6.01 | Log4j2, console only, dynamic level via config |
| DEPL-7.01 | Graceful shutdown: EtlProcessor.shutdown() with awaitTermination |

### UIX — Web GUI

| ID | Description |
|----|-------------|
| UIX-1.01 | Bootstrap Table dataset browser at /dataset/{id}/html |
| UIX-2.01 | Full mode: navbar, header, table, footer |
| UIX-2.02 | Embed mode (?embed): minimal, iframe-friendly |
| UIX-3.01 | Server-side pagination via _limit/_offset |
| UIX-3.02 | Per-column filter inputs (Enter to apply) |
| UIX-3.03 | Column visibility toggle |
| UIX-3.04 | Refresh button |
| UIX-3.05 | Dynamic column generation from info endpoint |
| UIX-4.01 | Swagger documentation link |
| UIX-5.01 | Accept: text/html redirects 303 to /html |

### TEST — Testing

| ID | Description |
|----|-------------|
| TEST-1.01 | Integration tests verifying REST API end-to-end |
| TEST-2.01 | JUnit 5, REST Assured, Testcontainers, Awaitility, AssertJ |
| TEST-3.01 | 22 IT classes, 141 test methods |
| TEST-3.02 | Sequential execution within classes |
| TEST-4.01 | Default profile (regexpqueries=full) |
| TEST-4.02 | regex-disabled profile |
| TEST-4.03 | regex-simple profile |
| TEST-4.04 | ratelimit profile |
| TEST-5.01 | External mode: -Drowstore.baseUrl |
| TEST-6.01 | Awaitility polling for async ETL |
| TEST-7.01 | 12+ CSV test files covering encodings and edge cases |
| TEST-8.01 | Endpoint × test spec coverage matrix |
| TEST-9.01 | specs/tests/: 121 test cases, 9 files |

### USR — User Stories

| ID | Description |
|----|-------------|
| USR-1.01 | Upload CSV dataset |
| USR-1.02 | Update/replace dataset |
| USR-1.03 | Manage aliases |
| USR-1.04 | Check processing status |
| USR-1.05 | Delete dataset |
| USR-2.01 | Discover datasets |
| USR-2.02 | Query with column filters |
| USR-2.03 | Paginate results |
| USR-2.04 | Use regex filters |
| USR-2.05 | Export full dataset |
| USR-2.06 | [REMOVED] Use JSONP for cross-origin — replaced by CORS |
| USR-2.07 | Get OpenAPI spec |
| USR-2.08 | Use CORS for cross-origin access |
| USR-3.01 | Deploy standalone instance |
| USR-3.02 | Configure rate limiting |
| USR-3.03 | Monitor status and JVM |
| USR-3.04 | Set up read replica |
| USR-3.05 | Configure query timeouts |
| USR-3.06 | Troubleshoot ETL errors |
| USR-4.01 | Browse in full-page GUI |
| USR-4.02 | Embed in iframe |
| USR-4.03 | Filter and paginate in GUI |

### GLO — Glossary

| ID | Description |
|----|-------------|
| GLO-1.01 | Alias |
| GLO-1.02 | Append Mode |
| GLO-1.03 | Column |
| GLO-1.04 | Data Table |
| GLO-1.05 | Dataset |
| GLO-1.06 | ETL |
| GLO-1.07 | ETL Status |
| GLO-1.08 | Export |
| GLO-1.09 | JSONB |
| GLO-1.10 | [REMOVED] JSONP — replaced by CORS |
| GLO-1.11 | Pagination |
| GLO-1.12 | Populate |
| GLO-1.13 | Purge |
| GLO-1.14 | Query |
| GLO-1.15 | Rate Limit (Average) |
| GLO-1.16 | Rate Limit (Sliding Window) |
| GLO-1.17 | Read Replica |
| GLO-1.18 | Regex Mode |
| GLO-1.19 | Replace Mode |
| GLO-1.20 | Row |
| GLO-1.21 | RowStore |
| GLO-1.22 | Swagger/OpenAPI |
| GLO-1.23 | CORS |
