# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
docker run -p 8282:8282 -v /path/to/rowstore.json:/srv/rowstore/rowstore.json rowstore
```

The image reads its configuration from `/srv/rowstore/rowstore.json` by default.

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
- `RowStoreConfigLoader` - Bridges `rowstore.json` into Spring: loads the file and exposes it as a `RowStoreConfig` bean (not `application.properties`)
- `DataSourceConfig` - Creates the primary and the read-only query `DataSource` programmatically from `rowstore.json` (`PGSimpleDataSource` by default, HikariCP when `connectionPoolMax` > 0) and runs Flyway migrations programmatically (Spring's DataSource and Flyway auto-configuration are disabled)
- `CorsConfig` - CORS filter (`FilterRegistrationBean`) with origins from `cors.allowedorigins`
- `DatabaseExceptionHandler` - Maps database connectivity failures in controllers to 503 (instead of 404/500)
- `RowStoreHealthIndicator` - Actuator health check (database connectivity, dataset count, active ETL processes)
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

`PgDataset.populate()` loads a CSV in a single transaction. An append inserts into the data table; a replace (PUT)
loads into a shadow table `<dataTable>_new` and swaps it for the data table at the end of the same transaction, so
the previous data is served during the load and kept if it fails (see ETL-6 in `specs/04-etl-pipeline.md`).
At startup, `PgRowStore` sets datasets left in PROCESSING to ERROR, drops leftover shadow tables, and starts a
background cleanup of column names stored before 2.0 with a byte order mark or soft hyphens (ETL-11).

**ETL Layer** (`etl/`):
- `EtlProcessor` - Per-dataset job queues: loads of a dataset run one at a time in submission order on a virtual
  thread, a replace discards the dataset's queued (not yet started) loads, and a `Semaphore` (`maxetlprocesses`)
  limits the number of running loads; `runExclusively()` runs other tasks (column name cleanup) in the same order
- `EtlResource` - ETL job wrapper
- `EtlStatus` - Status constants (CREATED=0, ACCEPTED_DATA=1, PROCESSING=2, AVAILABLE=3, ERROR=4)

**Filters** (`filters/`), in this order (CORS is registered in `CorsConfig`):
1. `PathParameterFilter` - Rejects request paths containing `;` (path parameters) with 400
2. `CorsFilter` - CORS headers, also on responses of the filters below (e.g. 429)
3. `TrustedProxyFilter` - Takes the client address from `X-Forwarded-For` only for peers listed in `trustedproxies`
4. `RateLimitFilter` - Request rate limiting (sliding window or average), active only when configured

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

Per-dataset data tables (`data_<uuid without hyphens>`) are created at runtime by `PgDatasets.createDataTable()`:
rows are stored as JSONB (`rownr BIGSERIAL PRIMARY KEY, data JSONB NOT NULL`). Tables created before 2.0 keep
`rownr SERIAL` until their next replace (PUT), which recreates the table with `BIGSERIAL`.

### Query Processing

Queries support regex matching (configurable via `regexpqueries`: disabled/simple/full). Simple mode requires a `^` prefix; full mode detects regular expressions heuristically. In full mode, prefix query values with `~` to force regex interpretation.

### Configuration

Set via `rowstore.json` (see resolution order above). Key settings: `baseurl`, `regexpqueries`, `maxetlprocesses`, `querytimeout`, `querymaxlimit`, `maxuploadsize`, `legacyparser`, `database`, `queryDatabase` (read replica), `cors`, `ratelimit`, `trustedproxies`. See `specs/06-configuration.md` for all options.

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
