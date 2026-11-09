# Testing

## Purpose

Documents the testing strategy, infrastructure, test organization, profiles, and coverage for RowStore.

## Scope

Covers the Java integration test suite and its relationship to the framework-agnostic test specifications in `specs/tests/`. For deployment of the test environment, see [08-deployment.md](08-deployment.md).

## TEST-1 Strategy Overview

> **TEST-1.01** RowStore uses integration tests that verify REST API behavior end-to-end. Each test starts a real RowStore instance backed by a PostgreSQL database, sends HTTP requests, and asserts on responses. Behaviour that is rich but not observable deterministically over HTTP, such as the scheduling of ETL jobs, is additionally covered by unit tests (`*Test.java`, Maven Surefire) using fakes instead of a database.

## TEST-2 Infrastructure

> **TEST-2.01** Test stack:
> - **JUnit Jupiter 6** — Test framework with `@TestMethodOrder(OrderAnnotation.class)` for sequential execution
> - **REST Assured 6.3.0** — HTTP client for sending requests and asserting responses
> - **Testcontainers 2.0.5** — Manages a PostgreSQL 16-alpine Docker container per test environment; another image can be set with `-Drowstore.test.postgresImage`, and CI additionally runs a subset of the integration tests against `postgres:9.6-alpine`, the oldest supported version (DEPL-3.01, DEPL-4.01)
> - **Awaitility 4.3.0** — Polling-based assertions for async ETL completion
> - **AssertJ 3.27.7** — Fluent assertion library

## TEST-3 Test Organization

> **TEST-3.01** The test suite contains **44 `*IT.java` integration test classes** with **386 tests** and **24 `*Test.java` unit test classes** with **324 tests** (as executed by Maven Failsafe and Surefire, including the tests of nested classes). Integration test classes follow the Maven Failsafe `*IT.java` naming convention, unit test classes the Maven Surefire `*Test.java` convention; `mvn test` runs only the unit tests, `mvn verify` runs both.

> **TEST-3.02** Tests within each class are executed **sequentially** using JUnit Jupiter's `@Order` annotation. This is necessary because many tests depend on prior test state (e.g., a dataset created in test 1 is queried in test 2).

## TEST-4 Test Profiles

> **TEST-4.01** **Default profile** (`regexpqueries: "full"`): The base test suite runs with full regex query support enabled.

> **TEST-4.02** **regex-disabled profile**: Tests that verify behavior when `regexpqueries` is set to `"disabled"`. Uses a separate `ConfigurableTestEnvironment`.

> **TEST-4.03** **regex-simple profile**: Tests that verify behavior when `regexpqueries` is set to `"simple"`. Uses a separate `ConfigurableTestEnvironment`.

> **TEST-4.04** **ratelimit profile**: Tests tagged with `@Tag("ratelimit")` that verify rate limiting behavior. Uses separate extensions with rate limit configuration enabled.

## TEST-5 External Testing Mode

> **TEST-5.01** Tests can run against an external RowStore instance by setting `-Drowstore.baseUrl=http://...`. When this property is set, Testcontainers is skipped — no Docker container is started. The tests send requests to the specified URL instead.

## TEST-6 Async Test Patterns

> **TEST-6.01** ETL processing is asynchronous, so tests that upload CSV data use Awaitility with an initial delay followed by polling:
> ```java
> await().initialDelay(INITIAL_DELAY)
>        .atMost(MAX_WAIT)
>        .pollInterval(POLL_INTERVAL)
>        .until(() -> /* check dataset status == AVAILABLE */);
> ```
> This pattern handles the variable processing time of the ETL pipeline.

## TEST-7 Test Data

> **TEST-7.01** The test suite uses CSV files covering various edge cases:
> - Standard CSV with commas
> - Semicolon-delimited CSV
> - UTF-8 encoded data
> - ISO-8859-1 (Latin-1) encoded data
> - Windows-1252 encoded data
> - CSV with quoted fields
> - CSV with empty fields
> - Large datasets for pagination testing
> - Malformed/corrupt CSV for error handling tests

## TEST-8 Coverage Matrix

> **TEST-8.01** Endpoint coverage:
>
> | Endpoint | Test Spec | IT Classes |
> |----------|-----------|------------|
> | `GET /status` | [01-status.md](tests/01-status.md) | StatusIT, StatusExtendedIT |
> | `GET /datasets` | [02-datasets.md](tests/02-datasets.md) | DatasetsListIT |
> | `POST /datasets` | [03-dataset-lifecycle.md](tests/03-dataset-lifecycle.md) | DatasetUtf8LifecycleIT |
> | `GET /dataset/{id}` | [05-querying.md](tests/05-querying.md) | QueryAdvancedIT, RegexDisabledIT, RegexSimpleIT |
> | `POST/PUT /dataset/{id}` | [03-dataset-lifecycle.md](tests/03-dataset-lifecycle.md) | DatasetUtf8LifecycleIT |
> | `DELETE /dataset/{id}` | [03-dataset-lifecycle.md](tests/03-dataset-lifecycle.md) | DatasetUtf8LifecycleIT |
> | `GET /dataset/{id}/info` | [07-metadata-endpoints.md](tests/07-metadata-endpoints.md) | DatasetUtf8LifecycleIT |
> | `*/dataset/{id}/aliases` | [07-metadata-endpoints.md](tests/07-metadata-endpoints.md) | DatasetUtf8LifecycleIT |
> | `GET /dataset/{id}/export` | [06-export.md](tests/06-export.md) | ExportIT |
> | `GET /dataset/{id}/html` | — | WebGuiIT |
> | `GET /dataset/{id}/swagger` | — | SwaggerIT |
> | CSV formats | [04-csv-formats.md](tests/04-csv-formats.md) | CsvParsingIT, CsvEmptyLineIT, CsvEmptyLineLegacyParserIT, CsvBomIT, CsvBomLegacyParserIT, DatasetEncodingIT, DatasetSemicolonIT, DatasetEmptyColumnIT, DatasetCorruptIT, CsvNoColumnsIT, CsvNoColumnsLegacyParserIT |
> | Error handling | [08-error-handling.md](tests/08-error-handling.md) | ErrorHandlingIT, DatasetLookupLoggingIT (plus unit test PgDatasetLookupTest) |
> | Rate limiting | [09-rate-limiting.md](tests/09-rate-limiting.md) | RateLimitIT, RateLimitAverageIT, RateLimitPartialConfigIT, RateLimitZeroValueIT, RateLimitClientIpIT (plus unit test TrustedProxyFilterTest) |
> | Path parameters | [08-error-handling.md](tests/08-error-handling.md) | PathParameterIT |
> | CORS | [15-cors.md](tests/15-cors.md) | CorsIT, CorsRestrictedIT, RateLimitClientIpIT (TC-CORS-007) |
> | Configuration and data sources | [10-configuration.md](tests/10-configuration.md) | ConnectionPoolIT (plus unit tests RowStoreConfigTest, DataSourceConfigTest) |
> | Connection limits | [12-connection-limits.md](tests/12-connection-limits.md) | ConnectionLimitIT, ConnectionLimitSingleSlotIT, ConnectionLimitHealthIT (plus unit tests LimitedDataSourceTest, RowStoreConfigConnectionLimitsTest, PgRowStoreSessionSqlTest) |
> | Deployment | [13-deployment.md](tests/13-deployment.md) | AccessLogIT (plus unit tests RowStoreApplicationTest, RowStoreConfigLoaderTest, AccessLogFilterTest) |
> | HTTP compatibility (content negotiation, writes to `/dataset/{id}/json`, method tunnel, compressed request bodies, `Range`) | [14-http-compatibility.md](tests/14-http-compatibility.md) | ContentNegotiationIT, DatasetJsonPathIT, MethodTunnelIT, CompressedUploadIT, UploadSizeLimitIT, RangeRequestIT (plus unit tests ContentNegotiationTest, TunnelFilterTest, RequestDecodingFilterTest, DatasetUtilTempFileTest) |
> | ETL processing | [11-etl-processing.md](tests/11-etl-processing.md) | EtlProcessingIT, DatasetReplaceIT, DatasetDeleteIT, DatasetDeleteSingleEtlIT, ColumnNameCleanupIT (plus unit tests EtlProcessorTest, PgDatasetPopulateTest, PgDatasetSwapLockTest, ColumnNamesTest, DatasetUtilBomReaderTest, PgRowStoreConnectionFailureTest) |

## TEST-9 Framework-Agnostic Specs

> **TEST-9.01** Framework-agnostic test specifications are maintained in [`specs/tests/`](tests/README.md). These contain **338 test cases** across 15 spec files (plus a README), counted as test case IDs without the retired (`[REMOVED]`) ones. Each spec defines test case IDs (`TC-{MODULE}-{NUMBER}`), request/response details, assertions, and retry logic. They serve as the authoritative definition of expected behavior, independent of the JUnit implementation.

## Known Limitations

- Unit tests cover selected components only (configuration parsing, ETL scheduling, column names, proxy handling); most testing is integration-level
- No performance or load tests
- No contract tests or API schema validation tests
- Test execution requires Docker for Testcontainers
- Sequential test execution within classes limits parallelism

## References

- [Test Specifications](tests/README.md) — Framework-agnostic test specifications
- [REST API](03-api.md#api-3-endpoint-catalog) — API endpoints under test
- [Deployment](08-deployment.md#depl-3-cicd) — CI/CD test pipeline
- [Glossary](12-glossary.md#glo-1-terms) — Term definitions
- Source: `BaseIntegrationTest.java`, `TestUtils.java`, `RowStoreExtension.java`, `ConfigurableTestEnvironment.java`

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: 22 IT classes, 141 test methods, expanded coverage matrix |
| 2026-10-20 | Unit tests for ETL scheduling and populate status handling, ETL processing ITs |
| 2026-10-20 | Atomic replace ITs and swap lock retry unit test |
| 2026-10-20 | Empty line ITs for the default and the legacy parser |
| 2026-10-20 | TEST-3.01, TEST-9.01 counts updated for the merged 2.0 test suite; TEST-8.01 rows for rate limiting via trusted proxies, path parameters and configuration |
| 2026-10-21 | TEST-3.01, TEST-9.01 counts updated after merging the connection budgets and the reserved `_callback` parameter; TEST-8.01 row for connection limits |
| 2026-10-23 | TEST-3.01, TEST-9.01 counts updated for the deployment fixes (ROWS-161, ROWS-163, ROWS-166); TEST-8.01 row for deployment |
| 2026-11-05 | TEST-2.01: PostgreSQL image selectable, CI subset against PostgreSQL 9.6 (ROWS-165) |
| 2026-11-05 | TEST-3.01, TEST-9.01 counts updated for the swap lock retries, the startup retry and the 503 handling |
| 2026-11-05 | TEST-3.01, TEST-9.01 counts updated; TEST-8.01: CORS test spec `15-cors.md` and CorsRestrictedIT (ROWS-170) |
| 2026-11-05 | TEST-3.01 counts updated from a full run (ROWS-169, ROWS-170); TEST-8.01: tests of logging unknown dataset IDs |
| 2026-11-09 | TEST-2.01: versions of the test stack with Spring Boot 4.1; TEST-3.01 count updated for a non-UTF-8 percent-encoding in TC-QUERY-022; TEST-3.02 names JUnit Jupiter (ROWS-143) |
| 2026-11-09 | TEST-3.01, TEST-9.01 counts updated for the pagination and `Range` test cases; TEST-8.01 row for HTTP compatibility (ROWS-177) |
| 2026-11-09 | TEST-3.01, TEST-9.01 counts updated from a full run (ROWS-171 to ROWS-176, ROWS-180) |
