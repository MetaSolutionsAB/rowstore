# Testing

## Purpose

Documents the testing strategy, infrastructure, test organization, profiles, and coverage for RowStore.

## Scope

Covers the Java integration test suite and its relationship to the framework-agnostic test specifications in `specs/tests/`. For deployment of the test environment, see [08-deployment.md](08-deployment.md).

## TEST-1 Strategy Overview

> **TEST-1.01** RowStore uses integration tests that verify REST API behavior end-to-end. Each test starts a real RowStore instance backed by a PostgreSQL database, sends HTTP requests, and asserts on responses. There are no separate unit tests — all testing is done at the integration level.

## TEST-2 Infrastructure

> **TEST-2.01** Test stack:
> - **JUnit 5** — Test framework with `@TestMethodOrder(OrderAnnotation.class)` for sequential execution
> - **REST Assured 5.4.0** — HTTP client for sending requests and asserting responses
> - **Testcontainers 1.20.4** — Manages a PostgreSQL 16-alpine Docker container per test environment
> - **Awaitility 4.2.0** — Polling-based assertions for async ETL completion
> - **AssertJ 3.27.7** — Fluent assertion library

## TEST-3 Test Organization

> **TEST-3.01** The test suite contains **22 `*IT.java` test classes** with **141 test methods**. All test classes follow the Maven Failsafe `*IT.java` naming convention.

> **TEST-3.02** Tests within each class are executed **sequentially** using JUnit 5's `@Order` annotation. This is necessary because many tests depend on prior test state (e.g., a dataset created in test 1 is queried in test 2).

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
> | CSV formats | [04-csv-formats.md](tests/04-csv-formats.md) | CsvParsingIT, DatasetEncodingIT, DatasetSemicolonIT, DatasetEmptyColumnIT, DatasetCorruptIT |
> | Error handling | [08-error-handling.md](tests/08-error-handling.md) | ErrorHandlingIT |
> | Rate limiting | [09-rate-limiting.md](tests/09-rate-limiting.md) | RateLimitIT, RateLimitAverageIT, RateLimitPartialConfigIT, RateLimitZeroValueIT |
> | CORS | — | CorsIT |

## TEST-9 Framework-Agnostic Specs

> **TEST-9.01** Framework-agnostic test specifications are maintained in [`specs/tests/`](tests/README.md). These contain **121 test cases** across 9 spec files (plus a README). Each spec defines test case IDs (`TC-{MODULE}-{NUMBER}`), request/response details, assertions, and retry logic. They serve as the authoritative definition of expected behavior, independent of the JUnit implementation.

## Known Limitations

- No unit tests (all testing is integration-level)
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
