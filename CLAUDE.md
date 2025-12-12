# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build (skip tests)
mvn -Dmaven.test.skip=true install

# Build and run integration tests (requires Docker for Testcontainers)
mvn clean verify

# Run standalone server
chmod +x standalone/jetty/target/dist/bin/rowstore
standalone/jetty/target/dist/bin/rowstore <config-file.json> [port]
```

## Testing

### Java Integration Tests (JUnit 5 + REST Assured + Testcontainers)

Integration tests automatically start a PostgreSQL container via Testcontainers and an embedded RowStore server. **Docker must be running.**

Docker 29+ compatibility is handled via `-Dapi.version=1.44` in the Maven plugin configuration (Docker 29 requires API version 1.44+).

```bash
# Run all integration tests (no manual setup required)
cd webapp
mvn verify

# Run integration tests only (skip unit tests)
mvn failsafe:integration-test failsafe:verify

# Run a single test class
mvn verify -Dit.test=StatusIT

# Run a single test method
mvn verify -Dit.test=DatasetUtf8LifecycleIT#getDatasetInfo_returnsValidStructure
```

To run tests against an external RowStore instance (skips Testcontainers):

```bash
mvn verify -Drowstore.baseUrl=http://localhost:8282
```

Test classes follow the `*IT.java` naming convention (Maven Failsafe).

### Legacy JavaScript Tests (Frisby/Jasmine)

Requires manual setup of PostgreSQL and RowStore:

```bash
# Start RowStore manually
standalone/jetty/target/dist/bin/rowstore --config tests/rowstore_tests_postgres.json --port 8282 &

# Run tests
cd tests
npm install frisby@0.8.5
jasmine-node .
```

### Test Specifications

Framework-agnostic test specifications are in `tests/specs/`. Each spec contains test case IDs, request/response details, assertions, and retry logic for async operations.

## Architecture

RowStore is a CSV-to-JSON pipeline with a REST query interface, storing tabular data in PostgreSQL's JSONB columns.

### Module Structure

- **webapp** - Core application (Restlet-based REST API, ETL pipeline, PostgreSQL storage)
- **standalone/jetty** - Embedded Jetty server for standalone deployment
- **standalone/common** - Shared standalone utilities

### Key Components

**Application Layer** (`org.entrystore.rowstore`):
- `RowStoreApplication` - Restlet application entry point, configures routes and filters

**Store Layer** (`org.entrystore.rowstore.store`):
- `RowStore` (interface) / `PgRowStore` (impl) - Main service facade
- `Dataset` (interface) / `PgDataset` (impl) - Dataset operations (query, populate, aliases)
- `Datasets` (interface) / `PgDatasets` (impl) - Dataset CRUD management
- `RowStoreConfig` - JSON configuration parsing

**ETL Layer** (`org.entrystore.rowstore.etl`):
- `EtlProcessor` - Queue-based async CSV processing with configurable concurrency
- `EtlResource` - ETL job wrapper
- `EtlStatus` - Status constants (CREATED=0, ACCEPTED_DATA=1, PROCESSING=2, AVAILABLE=3, ERROR=4)

**REST Resources** (`org.entrystore.rowstore.resources`):
- `DatasetResource` - GET (query with column filters), PUT/POST (upload CSV), DELETE
- `DatasetsResource` - GET (list), POST (create)
- `AliasResource`, `DatasetInfoResource`, `ExportResource`, `StatusResource`

**Filters** (`org.entrystore.rowstore.filters`):
- `RateLimitFilter` - Request rate limiting (sliding window or average)
- `JSCallbackFilter` - JSONP callback support
- `ApiKeyFilter` - API key authentication

### Database Schema

- `datasets` - Registry table (id UUID, status INT, created TIMESTAMP, data_table CHAR)
- `aliases` - Alias lookup (dataset_id UUID, alias TEXT)
- Per-dataset table - Rows stored as JSONB (rownr SERIAL, data JSONB)

### Query Processing

Queries support regex matching (configurable: disabled/simple/full). Simple mode requires `^` prefix; full mode accepts any regex. Prefix query values with `~` to force regex interpretation.

### Configuration

Set via `rowstore.json` or `ROWSTORE_CONFIG_URI` environment variable. Key settings: `baseurl`, `regexpqueries`, `maxetlprocesses`, `querytimeout`, `querymaxlimit`, `database`, `queryDatabase` (read replica), `ratelimit`.
