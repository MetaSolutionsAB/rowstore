# ROWSTORE CHANGELOG

## 2.0 (unreleased)

RowStore 2.0 is a rewrite of the application layer on Spring Boot. The REST API (paths, parameters, JSON responses) and the `rowstore.json` format stay compatible with 1.x, except for the changes listed in the upgrade notes below.

- Migration from Restlet to Spring Boot 3.5 with embedded Tomcat; a single Maven module builds an executable JAR, which replaces the WAR file and the standalone Jetty distribution
- Java 25 with virtual threads for request handling and ETL processing
- JSONP (`_callback`) removed, replaced by CORS with configurable origins (`cors.allowedorigins`)
- Database schema of the administrative tables managed by Flyway; existing 1.x databases are adopted at the first start
- HikariCP connection pooling (replacing `PGPoolingDataSource`) when `connectionPoolMax` > 0, for `database` and `queryDatabase`; no connection pool by default
- New option `maxuploadsize` limits the size of CSV uploads (default 100 MB, HTTP 413 above)
- Spring Boot Actuator endpoints (`/actuator/health`, `/actuator/info`, `/actuator/metrics`) and OpenAPI documentation (`/v3/api-docs`, `/swagger-ui.html`)
- Unit tests and Testcontainers-based integration tests replace the JavaScript test suite

### Improvement

[ROWS-91](https://metasolutions.atlassian.net/browse/ROWS-91) Remove UTF-8 BOM etc. from column names

[ROWS-112](https://metasolutions.atlassian.net/browse/ROWS-112) Use BIGSERIAL for rownr in new and shadow data tables

[ROWS-122](https://metasolutions.atlassian.net/browse/ROWS-122) Rewrite CLAUDE.md for the Spring Boot single-module layout

### Task

[ROWS-104](https://metasolutions.atlassian.net/browse/ROWS-104) Remove the ApiKeyFilter stub and document the reverse-proxy security contract

[ROWS-107](https://metasolutions.atlassian.net/browse/ROWS-107) Documentation truth pass for 2.0: README, CLAUDE.md and CHANGELOG

[ROWS-143](https://metasolutions.atlassian.net/browse/ROWS-143) Run a one-off dependency vulnerability scan before tagging 2.0 (pending, to be run on the release candidate)

### Bug

[ROWS-101](https://metasolutions.atlassian.net/browse/ROWS-101) Export corrupts rows with duplicate cell values, fails on sparse rows, and leaks a connection on DB error

[ROWS-102](https://metasolutions.atlassian.net/browse/ROWS-102) Fail fast on invalid or incomplete rowstore.json instead of silently degrading

[ROWS-103](https://metasolutions.atlassian.net/browse/ROWS-103) Client-supplied X-Forwarded-For bypasses per-client-IP rate limiting

[ROWS-105](https://metasolutions.atlassian.net/browse/ROWS-105) populate() swallows in-flight exceptions with a return inside finally

[ROWS-106](https://metasolutions.atlassian.net/browse/ROWS-106) set-version.sh writes VERSION.txt to the removed webapp module path

[ROWS-114](https://metasolutions.atlassian.net/browse/ROWS-114) PUT replace is not atomic: dataset is empty during re-import and data is lost if the import fails

[ROWS-138](https://metasolutions.atlassian.net/browse/ROWS-138) An empty line in a CSV silently drops every row after it, and the load reports success

[ROWS-139](https://metasolutions.atlassian.net/browse/ROWS-139) Swagger endpoint returns invalid JSON or fails when a column name contains a backslash, double quote or dollar sign

[ROWS-140](https://metasolutions.atlassian.net/browse/ROWS-140) A DB outage during a load leaves a dataset stuck in PROCESSING, and the endless wait for it blocks every other upload

[ROWS-141](https://metasolutions.atlassian.net/browse/ROWS-141) Datasource settings from 1.7 and develop lost in the Spring migration (SSL mode, driver timeouts with HikariCP, read-only query connections)

[ROWS-144](https://metasolutions.atlassian.net/browse/ROWS-144) Path parameters (';') bypass proxy path rules and the per-dataset rate limit

[ROWS-145](https://metasolutions.atlassian.net/browse/ROWS-145) Rate-limited responses (429) carry no CORS headers, so browsers cannot read Retry-After

### Upgrade notes

Changes visible to operators and clients when upgrading from 1.7:

- **Deployment:** RowStore is started with `java -jar rowstore-<version>.jar --rowstore.config.uri=file:///path/to/rowstore.json` (or the environment variable `ROWSTORE_CONFIG_URI`); the port is set with `--server.port=<port>` (default 8282). The WAR file, the standalone start script `bin/rowstore <config> [port]` and the options `-l/--log-level` and `--connector-params` (`ROWSTORE_CONNECTOR_PARAMS`) are gone. The Docker image reads `/srv/rowstore/rowstore.json`. Java 25 is required.
- **Reverse proxy required:** there is no application-level authentication (the `ApiKeyFilter` stub was removed). The reverse proxy must authenticate all requests other than `GET`/`HEAD` and block `/actuator/**`, `/swagger-ui*`, `/v3/api-docs*` and `/status?jvm`, which are served on the application port until a separate management port is introduced in 2.1 ([ROWS-108](https://metasolutions.atlassian.net/browse/ROWS-108)). See the Security section of the README.
- **JSONP removed:** responses are never wrapped in a callback; like any parameter that is not a column name, `_callback` makes a dataset query fail with HTTP 400. Browser clients of other origins use CORS (`cors.allowedorigins`, default all origins).
- **Delete:** `DELETE /dataset/{id}` returns 204 No Content instead of 200 OK.
- **Fail-fast configuration:** configurations that 1.x tolerated now prevent the startup, with an error naming the offending key: missing `baseurl`, `database` or `database.host`/`database`/`user`/`password` (also in `queryDatabase`, if present), these keys with a non-string value, `database`/`queryDatabase`/`ratelimit`/`cors` that are not JSON objects, `cors.allowedorigins` or `trustedproxies` that are not arrays of strings, `trustedproxies` entries that are not IP addresses or CIDR ranges, and `querymaxlimit` or `maxuploadsize` not greater than 0. `exportpagesize` was never used and is ignored.
- **Database connection:** `"ssl": false` now disables SSL explicitly (`sslmode=disable`; 1.7 used the driver default `prefer`), so a server that requires SSL refuses the connection. The driver timeouts `socketTimeout`, `connectTimeout` and `loginTimeout` are applied with and without connection pool; 0 means unlimited. Connection pooling is configured with `connectionPoolMax` (HikariCP) and is off by default.
- **Column names of existing datasets:** column names that 1.x stored with a byte order mark or soft hyphens (e.g. `\ufeff"kopare_id"` instead of `kopare_id`) are renamed automatically in the background after the first start, one dataset at a time; datasets whose cleaned names would collide are skipped and logged. Queries, `/info`, Swagger and exports then use the cleaned names; queries by the old names stop working. New uploads always yield cleaned names.
- **Empty lines in CSV files:** an empty line between rows (or directly after the header) now fails the load (status 4, "Error"); 1.7 silently dropped all following rows (default parser) or loaded an empty row (legacy parser). Trailing empty lines are ignored, whitespace-only lines are rows, and an empty value in a single-column CSV must be quoted (`""`).
- **Replace (PUT):** the previous data is served until the replace is complete and is kept if the replace fails (status 4 with the previous data). The database needs disk space for the old and the new data during a replace. Like 1.x, the replace waits for long-running readers of the dataset's table (e.g. `pg_dump`), and new queries of the dataset wait behind it.
- **Upload order:** uploads to the same dataset are processed one after another in the order of arrival; a replace discards the dataset's uploads that are still waiting. `activeEtlProcesses` in `/status` counts only running loads. Datasets left in status 2 ("Processing") by a crash are set to 4 ("Error") at startup.
- **Database outages:** requests that cannot reach the database get HTTP 503 instead of 404.
- **Per-client rate limit:** `X-Forwarded-For` is only evaluated for requests from trusted proxies (`trustedproxies`, default: loopback and private address ranges). A reverse proxy on a public IP address must be listed in `trustedproxies`, otherwise all clients share the proxy's limit.
- **Path parameters:** request paths containing `;` are rejected with HTTP 400.

## 1.7 (2025-07-01)

### Improvement

[ROWS-93](https://metasolutions.atlassian.net/browse/ROWS-93) Switch to RFC4180Parser by default

[ROWS-94](https://metasolutions.atlassian.net/browse/ROWS-94) Enable logging for PostgreSQL driver

[ROWS-95](https://metasolutions.atlassian.net/browse/ROWS-95) Add settings for DB timeouts and set default values

[ROWS-98](https://metasolutions.atlassian.net/browse/ROWS-98) Bump dependencies to newer versions

[ROWS-99](https://metasolutions.atlassian.net/browse/ROWS-99) Remove support for Simple backend

## 1.6 (2024-02-07)

### New Feature

[ROWS-90](https://metasolutions.atlassian.net/browse/ROWS-90) Add support for exporting a dataset in CSV format

## 1.5 (2023-02-09)

### Improvement

[ROWS-88](https://metasolutions.atlassian.net/browse/ROWS-88) Detection of character encoding gives sometimes wrong result

[ROWS-89](https://metasolutions.atlassian.net/browse/ROWS-89) Upgrade to Java 17

## 1.4 (2021-12-11)

### New Feature

[ROWS-86](https://metasolutions.atlassian.net/browse/ROWS-86) External libs, css and fonts in Tabular Data Explorer should be served from own infrastructure

### Bug

[ROWS-84](https://metasolutions.atlassian.net/browse/ROWS-84) HTML templates cannot be loaded from any URL

[ROWS-85](https://metasolutions.atlassian.net/browse/ROWS-85) Swagger template cannot be loaded from JAR

## 1.3 (2021-02-17)

### Improvement

[ROWS-36](https://metasolutions.atlassian.net/browse/ROWS-36) Improve handling of command line parameters

[ROWS-57](https://metasolutions.atlassian.net/browse/ROWS-57) Introduce database connection pool

[ROWS-59](https://metasolutions.atlassian.net/browse/ROWS-59) Enable rate limiting per client IP address

[ROWS-62](https://metasolutions.atlassian.net/browse/ROWS-62) Exclude status resource from rate limitation

[ROWS-63](https://metasolutions.atlassian.net/browse/ROWS-63) Allow configuration of maximum result size limit

[ROWS-64](https://metasolutions.atlassian.net/browse/ROWS-64) Replace Apache Tika with another way of detecting the charset of files

[ROWS-65](https://metasolutions.atlassian.net/browse/ROWS-65) Remove footer from tabular search interface

[ROWS-67](https://metasolutions.atlassian.net/browse/ROWS-67) Add additional properties to query responses

[ROWS-68](https://metasolutions.atlassian.net/browse/ROWS-68) Add Retry-After header to responses with HTTP Status 429 Too Many Requests

[ROWS-69](https://metasolutions.atlassian.net/browse/ROWS-69) Replace FileInputStream and FileOutputStream with methods in Files class

[ROWS-76](https://metasolutions.atlassian.net/browse/ROWS-76) Change Maven build target to Java 11

[ROWS-77](https://metasolutions.atlassian.net/browse/ROWS-77) Allow to load configuration from URL

[ROWS-78](https://metasolutions.atlassian.net/browse/ROWS-78) Add Jetty support

[ROWS-81](https://metasolutions.atlassian.net/browse/ROWS-81) Consolidate logging

### Task

[ROWS-51](https://metasolutions.atlassian.net/browse/ROWS-51) Prepare for breaking changes in Java 10

[ROWS-82](https://metasolutions.atlassian.net/browse/ROWS-82) Update NOTICE.txt

### New Feature

[ROWS-53](https://metasolutions.atlassian.net/browse/ROWS-53) Keep track of access statistics

[ROWS-58](https://metasolutions.atlassian.net/browse/ROWS-58) Support multiple read replicas and prepare for fast failover

[ROWS-80](https://metasolutions.atlassian.net/browse/ROWS-80) Provide memory stats via API

### Bug

[ROWS-52](https://metasolutions.atlassian.net/browse/ROWS-52) Broken stream handling with Restlet 2.4.1 behind reverse proxy

[ROWS-60](https://metasolutions.atlassian.net/browse/ROWS-60) Link to dataset yields download instead of HTML UI on IE

[ROWS-83](https://metasolutions.atlassian.net/browse/ROWS-83) Ignore empty column labels in first line

## 1.2 (2018-07-06)

### Improvement

[ROWS-39](https://metasolutions.atlassian.net/browse/ROWS-39) Add usage information to HTML interface

[ROWS-40](https://metasolutions.atlassian.net/browse/ROWS-40) Provide examples on HTML preview of API

[ROWS-41](https://metasolutions.atlassian.net/browse/ROWS-41) Make sure to trim column names before processing

[ROWS-45](https://metasolutions.atlassian.net/browse/ROWS-45) Make sure JDBC driver is deregistered upon context shutdown

[ROWS-48](https://metasolutions.atlassian.net/browse/ROWS-48) Add standard URL parameters to Swagger definition

[ROWS-49](https://metasolutions.atlassian.net/browse/ROWS-49) Add "next" pointer to paginated search results

### Task

[ROWS-43](https://metasolutions.atlassian.net/browse/ROWS-43) Check whether case sensitivity handling of column names can be a problem for additions to datasets

### New Feature

[ROWS-46](https://metasolutions.atlassian.net/browse/ROWS-46) Add possibility to use different backends for read and write operations

[ROWS-47](https://metasolutions.atlassian.net/browse/ROWS-47) Add Swagger UI as API documentation and link to it from HTML view

### Bug

[ROWS-42](https://metasolutions.atlassian.net/browse/ROWS-42) An unsuccessful API update must not leave the API disfunctional

# 1.1 (2017-06-05)

### Improvement

[ROWS-29](https://metasolutions.atlassian.net/browse/ROWS-29) Add pagination support to query results

[ROWS-31](https://metasolutions.atlassian.net/browse/ROWS-31) Move to Java 8

[ROWS-34](https://metasolutions.atlassian.net/browse/ROWS-34) Avoid to perform regexp queries if query string is not a regexp

[ROWS-35](https://metasolutions.atlassian.net/browse/ROWS-35) Introduce two levels of regexp support

### New Feature

[ROWS-1](https://metasolutions.atlassian.net/browse/ROWS-1) Support for JSON-LD for the info resource

[ROWS-23](https://metasolutions.atlassian.net/browse/ROWS-23) Add support for providing path to configuration file via environment variable

[ROWS-27](https://metasolutions.atlassian.net/browse/ROWS-27) Auto-create an HTML GUI for APIs \(search form and result view\)

[ROWS-32](https://metasolutions.atlassian.net/browse/ROWS-32) Support configuration of query timeouts

### Bug

[ROWS-30](https://metasolutions.atlassian.net/browse/ROWS-30) CSV files with Windows line breaks may not be processed completely

## 1.0 (2016-10-18)

### Improvement

[ROWS-3](https://metasolutions.atlassian.net/browse/ROWS-3) Make sure there is basic test coverage

[ROWS-18](https://metasolutions.atlassian.net/browse/ROWS-18) Provide simple detection for semi-colon separated files

[ROWS-19](https://metasolutions.atlassian.net/browse/ROWS-19) Auto-detect file encoding to avoid corrupt data in data table

[ROWS-21](https://metasolutions.atlassian.net/browse/ROWS-21) Keys in queries should be matched case insensitively

### New Feature

[ROWS-9](https://metasolutions.atlassian.net/browse/ROWS-9) Support for replacing data of existing dataset

[ROWS-11](https://metasolutions.atlassian.net/browse/ROWS-11) Support slugs for datasets

[ROWS-12](https://metasolutions.atlassian.net/browse/ROWS-12) Support for replacing data of a dataset

[ROWS-13](https://metasolutions.atlassian.net/browse/ROWS-13) Support for appending data to an existing dataset

[ROWS-15](https://metasolutions.atlassian.net/browse/ROWS-15) Provide request rate limitation per dataset

[ROWS-16](https://metasolutions.atlassian.net/browse/ROWS-16) Auto-generate Swagger description for each dataset

[ROWS-20](https://metasolutions.atlassian.net/browse/ROWS-20) Add possibility to set aliases for datasets

### Bug

[ROWS-22](https://metasolutions.atlassian.net/browse/ROWS-22) Queries with non-matching keys should yield a bad request error

[ROWS-30](https://metasolutions.atlassian.net/browse/ROWS-30) CSV files with Windows line breaks may not be processed completely

## 0.9 (2016-06-28)

### Improvement

[ROWS-8](https://metasolutions.atlassian.net/browse/ROWS-8) Improve indexing process to ignore fields that are too large
