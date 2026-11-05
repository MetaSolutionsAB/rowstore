# AGENTS.md

This file provides guidance to AI coding agents (e.g. Claude Code, Codex) when working with code in this repository.
`CLAUDE.md` imports it.

## Build Commands

All commands run from the repository root (single Maven module).

```bash
# Build (skip tests)
mvn -Dmaven.test.skip=true install

# Build and run all tests (integration tests require Docker for Testcontainers)
mvn clean verify

# Run the executable JAR
java -jar target/rowstore-<version>.jar --rowstore.config.uri=file:///path/to/rowstore.json

# Alternatively, pass the config URI via environment variable
ROWSTORE_CONFIG_URI=file:///path/to/rowstore.json java -jar target/rowstore-<version>.jar
```

Configuration is resolved in this order (see `RowStoreConfigLoader`): `--rowstore.config.uri` property,
`ROWSTORE_CONFIG_URI` environment variable, `rowstore.json` on the classpath. The default HTTP port is 8282
(`server.port` in `src/main/resources/application.properties`, override with `--server.port=<port>`).
An example configuration is in `src/main/resources/rowstore.json_example`. For local runs, `docker compose up -d`
starts PostgreSQL 16 (`docker-compose.yml`) matching `rowstore.example.json` (see Quickstart in `README.md`).

Docker image (the `Dockerfile` copies `target/rowstore-*.jar`, so build the JAR first):

```bash
mvn -Dmaven.test.skip=true install
docker build -t rowstore .
docker run -p 8282:8282 -v /path/to/rowstore.json:/srv/rowstore/rowstore.json --stop-timeout 70 rowstore
```

The image reads its configuration from `/srv/rowstore/rowstore.json` (env `ROWSTORE_CONFIG_URI`), runs as the
non-root user `rowstore` (UID 10001), honours `JAVA_OPTS` and passes `docker run` arguments to RowStore (DEPL-2.04).
`main` rejects arguments that are not of the form `--name=value` (CFG-7.05), so 1.x options do not pass silently.

Release versions are set with `./set-version.sh <version>`, which updates `pom.xml`, `VERSION.txt` and
`src/main/resources/VERSION.txt` (served by `/status`).

## Testing

### Java Integration Tests (JUnit 5 + REST Assured + Testcontainers)

Integration tests automatically start a PostgreSQL container via Testcontainers and an embedded RowStore server. **Docker must be running.**

Docker 29+ compatibility is handled via `-Dapi.version=1.44` in the Maven plugin configuration (Docker 29 requires API version 1.44+).

```bash
# Run all tests (no manual setup required)
mvn verify

# Run unit tests only (Maven Surefire, *Test.java, no Docker needed)
mvn test

# Build without running integration tests
mvn install -DskipITs

# Run a single test class
mvn verify -Dit.test=StatusIT

# Run a single test method
mvn verify -Dit.test=StatusIT#getStatus_returnsValidStructure
```

To run tests against an external RowStore instance (skips Testcontainers):

```bash
mvn verify -Drowstore.baseUrl=http://localhost:8282
```

Tests that need direct database access (`BaseIntegrationTest.openDatabaseConnection()`) are skipped in that mode.

Integration test classes follow the `*IT.java` naming convention (Maven Failsafe), unit tests `*Test.java` (Maven Surefire).
`mvn test` runs only the unit tests (e.g. `RowStoreConfigTest`, `EtlProcessorTest`, `TrustedProxyFilterTest`,
`ColumnNamesTest`); they use fakes or mocks instead of a database. `mvn verify` runs both.
Most ITs extend `BaseIntegrationTest` (shared environment from `RowStoreTestEnvironment`); ITs that need a
differently configured server extend `ConfigurableTestBase` with a dedicated extension (e.g. `RateLimitExtension`).
Test CSV files are in `src/test/resources/data/`.

### Test Specifications

Framework-agnostic test specifications are in `specs/tests/`. Each spec contains test case IDs, request/response details, assertions, and retry logic for async operations.

## Architecture

RowStore is a CSV-to-JSON pipeline with a REST query interface, storing tabular data in PostgreSQL's JSONB columns.
It is a Spring Boot 3.5 application (Java 25, embedded Tomcat, virtual threads) built as a single Maven module that
produces an executable JAR. See `specs/01-system-architecture.md` for the full architecture specification.

### Key Components

All packages are below `org.entrystore.rowstore` in `src/main/java`.

**Application** (`org.entrystore.rowstore`):
- `RowStoreApplication` - Spring Boot entry point (`@SpringBootApplication`)

**Configuration** (`config/`):
- `RowStoreConfigLoader` - Bridges `rowstore.json` into Spring: loads the file and exposes it as a `RowStoreConfig` bean (not `application.properties`); configuration failures are `RowStoreConfigException`s, reported without stack trace by `RowStoreConfigFailureAnalyzer` (CFG-1.03)
- `DataSourceConfig` - Creates the primary and the read-only query `DataSource` programmatically from `rowstore.json` (`PGSimpleDataSource` by default, HikariCP when `connectionPoolMax` > 0) and runs Flyway migrations programmatically (Spring's DataSource and Flyway auto-configuration are disabled); at startup, `StartupRetry` waits for a database that is not reachable yet (ARCH-7.08)
- `CorsConfig` - CORS filter (`FilterRegistrationBean`) with origins from `cors.allowedorigins`
- `DatabaseExceptionHandler` - Maps database connectivity failures in controllers to 503 with `Retry-After` (instead of 404/500)
- `RowStoreHealthIndicator` - Actuator health check of both databases within the query budgets (UP, BUSY if all connections are in use, DOWN if unreachable), dataset count, active and queued ETL processes, connection usage; replaces the disabled Actuator `db` indicator
- `CustomErrorController` - Error responses for `/error`

**REST Controllers** (`resources/`):
- `DatasetController` - GET `/dataset/{id}` (query with column filters, JSON or HTML), PUT/POST (replace/append CSV), DELETE
- `DatasetsController` - GET `/datasets` (list), POST (create from CSV)
- `AliasController`, `DatasetInfoController`, `ExportController`, `StatusController`, `SwaggerController`, `WebGuiController`
- `VersionInfo` - Reads `VERSION.txt` from the classpath
- `resources/model/` - Typed response records (`StatusResponse`, `JvmStatusResponse`, `DatasetInfoResponse`, `DatasetAcceptedResponse`, `QueryResponse`)

**Store Layer** (`store/`, implementations in `store/impl/`):
- `RowStore` (interface) / `PgRowStore` (impl) - Main service facade
- `Dataset` (interface) / `PgDataset` (impl) - Dataset operations (query, populate, aliases, export stream)
- `Datasets` (interface) / `PgDatasets` (impl) - Dataset CRUD management
- `RowStoreConfig` - JSON configuration parsing and fail-fast validation (invalid `rowstore.json` prevents startup)
- `QueryResult` - Query result envelope
- `RowStream` - Closeable cursor over all rows of a dataset, used by the streamed export
- `LimitedDataSource` (`store/impl/`) - Connection budget: limits the open connections of a data source, waits in
  FIFO order up to `connectionWaitTimeout`, then fails (mapped to 503)

**Connection budgets** (ARCH-7 in `specs/01-system-architecture.md`): `PgRowStore` wraps the data sources from
`DataSourceConfig` in `LimitedDataSource` budgets per database block. Use the matching accessor of `RowStore`:
`getQueryConnection()` / `getQueryJdbcTemplate()` for short operations of requests (queries, lookups, alias and
status changes by requests; `querytimeout` and `queryLockTimeout` apply), `getExportConnection()` for streamed
exports, and `getLoadConnection()` / `getLoadJdbcTemplate()` for everything an ETL job does, including its status
writes. New code must hold at most one connection per request or ETL job at a time (ARCH-7.04): do reads that belong
to an operation on the connection it already holds, or before/after it; otherwise requests can deadlock on an
exhausted budget until the wait timeout. Budget usage is reported by `/status` (`connections`) and the health indicator.

`PgDataset.populate()` loads a CSV in a single transaction. An append inserts into the data table; a replace (PUT)
loads into a shadow table `<dataTable>_new` and swaps it for the data table at the end of the same transaction, so
the previous data is served during the load and kept if it fails (see ETL-6 in `specs/04-etl-pipeline.md`).
At startup, `PgRowStore` sets datasets left in PROCESSING or ACCEPTED_DATA to ERROR (ETL-8.06), drops leftover shadow tables, and starts a
background cleanup of column names stored before 2.0 with a byte order mark or soft hyphens (ETL-11), unless a
completed run is recorded in `maintenance_tasks` (ETL-11.07).

**ETL Layer** (`etl/`):
- `EtlProcessor` - Per-dataset job queues: loads of a dataset run one at a time in submission order on a virtual
  thread, a replace discards the dataset's queued (not yet started) loads, and a `Semaphore` (`maxetlprocesses`)
  limits the number of running loads; `runExclusively()`/`callExclusively()` run other tasks (column name cleanup) in the same order
- `EtlResource` - ETL job wrapper
- `EtlStatus` - Status constants (CREATED=0, ACCEPTED_DATA=1, PROCESSING=2, AVAILABLE=3, ERROR=4)

**Filters** (`filters/`), in this order (CORS is registered in `CorsConfig`):
1. `AccessLogFilter` - Access log in the 1.x format after the response is complete, also for async responses (DEPL-6.02); resolves the client address through `TrustedProxyFilter`, so that requests rejected by the filters below are logged with it
2. `PathParameterFilter` - Rejects request paths containing `;` (path parameters) with 400
3. `CorsFilter` - CORS headers, also on responses of the filters below (e.g. 429)
4. `TrustedProxyFilter` - Takes the client address from `X-Forwarded-For`: by default the last entry, written by the connecting peer (the reverse proxy); with `trustedproxies`, only for listed peers, walked from the right
5. `RateLimitFilter` - Request rate limiting (sliding window or average), active only when configured

**Utilities** (`util/`):
- `DatasetUtil` - Dataset URL building, charset detection, byte order mark skipping, regex heuristics and safety checks
- `ColumnNames` - Normalization of CSV labels and stored column names (lowercase, strip, byte order marks and soft hyphens removed)
- `Hashing` - Hash helpers

### Security Model

RowStore has no application-level authentication; it must run behind a reverse proxy that authenticates all
non-GET/HEAD requests and blocks Actuator, Swagger UI, `/v3/api-docs` and `/status?jvm` (contract in the Security section of `README.md`).

### Database Schema

Registry tables are managed by Flyway (`src/main/resources/db/migration`):
- `datasets` - Registry table (id UUID, status INT, created TIMESTAMP, data_table CHAR(37))
- `aliases` - Alias lookup (id SERIAL, dataset_id UUID, alias TEXT)
- `maintenance_tasks` - Completed one-time maintenance tasks (name TEXT, completed TIMESTAMP, DOM-4.04), e.g. the
  cleanup of column names stored before 2.0. New one-time startup tasks should record their completion here with
  `MaintenanceTasks` under their own name instead of repeating a check at every startup.

Per-dataset data tables (`data_<uuid without hyphens>`) are created at runtime by `PgDatasets.createDataTable()`:
rows are stored as JSONB (`rownr BIGSERIAL PRIMARY KEY, data JSONB NOT NULL`). Tables created before 2.0 keep
`rownr SERIAL` until their next replace (PUT), which recreates the table with `BIGSERIAL`.

### Query Processing

Queries support regex matching (configurable via `regexpqueries`: disabled/simple/full). Simple mode requires a `^` prefix; full mode detects regular expressions heuristically. In full mode, prefix query values with `~` to force regex interpretation.

### Configuration

Set via `rowstore.json` (see resolution order above). Key settings: `baseurl`, `regexpqueries`, `maxetlprocesses`, `querytimeout`, `querymaxlimit`, `maxuploadsize`, `legacyparser`, `database`, `queryDatabase` (read replica; both with the connection limits `maxQueryConnections`, `maxExportConnections`, `connectionWaitTimeout`, `queryLockTimeout`), `cors`, `ratelimit`, `trustedproxies`. See `specs/06-configuration.md` for all options.

## Pitfalls

Behaviour that is easy to break or to rely on by mistake:

- **Data tables are dropped by every replace.** A replace (PUT) loads into a shadow table and drops the data table
  (ETL-6.02). Only the owner and the table privileges are carried over (`PgDataset.copyOwnerAndPrivileges()`);
  everything else attached to a data table is lost: manually created indexes, triggers, comments, storage parameters,
  column privileges, membership in publications that list the table. Views on a data table make every replace fail.
  Do not keep state on a data table; use the registry tables (`datasets`, `aliases`, Flyway migrations).
- **One RowStore instance per database.** ETL queues are in memory, a dataset in PROCESSING without a load in this
  instance is taken over, and startup resets PROCESSING and ACCEPTED_DATA to ERROR (ETL-8.05, ETL-8.06). Code must not assume
  coordination between instances (ROWS-149).
- **The final status of a load must not depend on reading the database.** `populate()` tracks the outcome locally and
  writes AVAILABLE or ERROR with retries (ETL-10.01), because the database may be unreachable when a load fails.
  Never put a `return` into a `finally` block that could swallow an exception.
- **`CSVReader.readNext()` returning `null` is not the end of the file.** `RFC4180Parser` returns `null` for an empty
  line; the end of the file is reached when `getLinesRead()` no longer advances (ETL-4.05).
- **The client address is `getRemoteAddr()`.** `TrustedProxyFilter` resolves it from `X-Forwarded-For`; never read
  that header elsewhere, and keep `server.forward-headers-strategy=none` (SEC-5.05, DEPL-1.04).
- **Stacked branches can merge cleanly but not compile.** Feature branches are often based on other open branches.
  After merging, run `mvn -DskipTests test-compile`: textual merges can add a method twice or leave a test fake
  implementing an interface method with an outdated signature.

## Specification Requirement IDs

Spec files in `specs/` use permanent requirement IDs in the format `{PREFIX}-{section}.{number}`
(e.g., API-3.02, SEC-4.01). These IDs are referenced from code comments, issues, and other docs.

Rules:
- Existing IDs must NEVER be changed — new requirements always use the next available number
- New requirements within a section: use next number (e.g., after SEC-4.06, add SEC-4.07)
- New sections: use next section number (e.g., after ARCH-6, add ARCH-7)
- Removed requirements keep their ID with [REMOVED] marker — never reassign
- See specs/README.md for the full ID registry and prefix table

## Git Commits

Do not include any AI or agentic involvement information in commits. Specifically:
- Do not add `Co-Authored-By` lines mentioning Claude, AI assistants, or similar
- Do not mention AI assistance in commit messages
- Write commit messages as if authored solely by the human developer
