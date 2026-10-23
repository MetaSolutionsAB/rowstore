# ROWSTORE CHANGELOG

## 2.0 (unreleased)

RowStore 2.0 is a rewrite of the application layer on Spring Boot. The REST API (paths, parameters, JSON responses) and the `rowstore.json` format stay compatible with 1.x, except for the changes listed in the upgrade notes below.

- Migration from Restlet to Spring Boot 3.5 with embedded Tomcat; a single Maven module builds an executable JAR, which replaces the WAR file and the standalone Jetty distribution
- Java 25 with virtual threads for request handling and ETL processing
- JSONP removed (`_callback` is accepted and ignored), replaced by CORS with configurable origins (`cors.allowedorigins`)
- Database schema of the administrative tables managed by Flyway; existing 1.x databases are adopted at the first start
- HikariCP connection pooling (replacing `PGPoolingDataSource`) when `connectionPoolMax` > 0, for `database` and `queryDatabase`; no connection pool by default
- Connection budgets limit the database connections per instance (`maxQueryConnections`, `maxExportConnections`, `connectionWaitTimeout`, `queryLockTimeout`); their usage is reported by `/status`
- New option `maxuploadsize` limits the size of CSV uploads (default 100 MB, HTTP 413 above)
- Spring Boot Actuator endpoints (`/actuator/health`, `/actuator/info`, `/actuator/metrics`) and OpenAPI documentation (`/v3/api-docs`, `/swagger-ui.html`)
- Unit tests and Testcontainers-based integration tests replace the JavaScript test suite

### Improvement

[ROWS-91](https://metasolutions.atlassian.net/browse/ROWS-91) Remove UTF-8 BOM etc. from column names

[ROWS-112](https://metasolutions.atlassian.net/browse/ROWS-112) Use BIGSERIAL for rownr in new and shadow data tables

[ROWS-162](https://metasolutions.atlassian.net/browse/ROWS-162) Docker image: honour JAVA_OPTS, keep the config URI when arguments are passed, run as non-root, document heap sizing

[ROWS-166](https://metasolutions.atlassian.net/browse/ROWS-166) Restore the per-request access log of 1.x

[ROWS-167](https://metasolutions.atlassian.net/browse/ROWS-167) Connection budgets: Retry-After and no per-request ERROR log on 503, no false Hikari leak warnings, warn when connectionPoolMax enables a pool

### Task

[ROWS-104](https://metasolutions.atlassian.net/browse/ROWS-104) Remove the ApiKeyFilter stub and document the reverse-proxy security contract

[ROWS-107](https://metasolutions.atlassian.net/browse/ROWS-107) Documentation truth pass for 2.0: README, CLAUDE.md and CHANGELOG

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

[ROWS-147](https://metasolutions.atlassian.net/browse/ROWS-147) Query with the 1.x reserved parameter _callback returns 400 instead of being ignored

[ROWS-148](https://metasolutions.atlassian.net/browse/ROWS-148) Unbounded concurrent database connections per instance with virtual threads (cap connections without idle pooling)

[ROWS-159](https://metasolutions.atlassian.net/browse/ROWS-159) A replace or column-name cleanup waiting up to 30 min for the table lock can exhaust the instance's query connections (503 for all datasets)

[ROWS-160](https://metasolutions.atlassian.net/browse/ROWS-160) RowStore does not start if the database is unreachable at startup (1.x started and recovered)

[ROWS-161](https://metasolutions.atlassian.net/browse/ROWS-161) Removed 1.x command line options (-c, -p, -l) are ignored silently, so RowStore starts with default port or fails with a stack trace

[ROWS-163](https://metasolutions.atlassian.net/browse/ROWS-163) Shutdown can take ~60 s (longer than Docker's stop timeout), and queued uploads leave datasets in ACCEPTED_DATA (1) forever

### Upgrade notes

Changes visible to operators and clients when upgrading from 1.7:

- **Deployment:** RowStore is started with `java -jar rowstore-<version>.jar --rowstore.config.uri=file:///path/to/rowstore.json` (or the environment variable `ROWSTORE_CONFIG_URI`; a plain file path works too); the port is set with `--server.port=<port>` (default 8282). The WAR file and the standalone start script `bin/rowstore <config> [port]` are gone. The options of 1.x prevent the startup with a message naming their replacement: `-c/--config` → `--rowstore.config.uri=<URI>`, `-p/--port` → `--server.port=<port>`, `-l/--log-level` → `loglevel` in rowstore.json, `--connector-params` → `maxQueryConnections` (connections are limited instead of threads); so does any other argument that is not of the form `--name=value`. `ROWSTORE_CONNECTOR_PARAMS` is ignored with a warning. A missing, unreadable or invalid configuration is reported as one error message without a stack trace (exit code 1). Java 25 is required.
- **Docker image:** the image reads `/srv/rowstore/rowstore.json` (set as the environment variable `ROWSTORE_CONFIG_URI`, so arguments passed to `docker run`, e.g. `--server.port=9000`, no longer replace it). JVM options are passed in `JAVA_OPTS`, as with the start script of 1.x; the maximum heap defaults to 75% of the container's memory limit. RowStore runs as the unprivileged user `rowstore` (UID/GID 10001) instead of root, so a mounted configuration must be readable for that user. `CLASSPATH_PREFIX` of the 1.x start script is not supported.
- **Shutdown:** on SIGTERM, running requests get up to 30 seconds, then the uploads that are still queued are discarded (logged with their datasets, they have to be uploaded again) and running loads get up to another 30 seconds before they are rolled back. Allow at least 70 seconds for the shutdown (`docker stop -t 70`, `stop_grace_period: 70s`, `terminationGracePeriodSeconds: 70`); Docker's default of 10 seconds kills RowStore while requests and loads are running.
- **Access log:** as in 1.x, every request is logged at INFO level in the same format (logger `org.entrystore.rowstore.LogService` instead of Restlet's `LogService`), with the client address resolved from `X-Forwarded-For` as for rate limiting; the new option `"accesslog": false` turns it off.
- **One instance per database:** a database must be used by only one RowStore instance at a time, also during deployments: stop the old instance before starting the new one. 1.7 roughly coordinated the loads of several instances on one database by polling the dataset status. 2.0 queues loads in memory per instance, takes over datasets in PROCESSING without waiting and resets PROCESSING to ERROR at startup, so a second instance on the same database could load a dataset concurrently or reset the loads of the other instance. Several instances can share a PostgreSQL cluster if each has its own database. Support for several instances per database is planned for 3.0 ([ROWS-149](https://metasolutions.atlassian.net/browse/ROWS-149)).
- **Reverse proxy required:** there is no application-level authentication (the `ApiKeyFilter` stub was removed). The reverse proxy must authenticate all requests other than `GET`/`HEAD` and block `/actuator/**`, `/swagger-ui*`, `/v3/api-docs*` and `/status?jvm`, which are served on the application port until a separate management port is introduced in 2.1 ([ROWS-108](https://metasolutions.atlassian.net/browse/ROWS-108)). See the Security section of the README.
- **JSONP removed:** the `_callback` parameter is accepted and ignored; responses are always plain JSON and never wrapped in a callback. Browser clients of other origins use CORS (`cors.allowedorigins`, default all origins).
- **Delete:** `DELETE /dataset/{id}` returns 204 No Content instead of 200 OK.
- **Fail-fast configuration:** configurations that 1.x tolerated now prevent the startup, with an error naming the offending key: missing `baseurl`, `database` or `database.host`/`database`/`user`/`password` (also in `queryDatabase`, if present), these keys with a non-string value, `database`/`queryDatabase`/`ratelimit`/`cors` that are not JSON objects, `cors.allowedorigins` or `trustedproxies` that are not arrays of strings, `trustedproxies` entries that are not IP addresses or CIDR ranges, `querymaxlimit` or `maxuploadsize` not greater than 0, and the new connection limits with invalid values (see below). `exportpagesize` was never used and is ignored.
- **Database connection:** `"ssl": false` now disables SSL explicitly (`sslmode=disable`; 1.7 used the driver default `prefer`), so a server that requires SSL refuses the connection. Without an `ssl` key, the driver default `prefer` still applies, as in 1.7.
- **Upload size:** CSV uploads larger than `maxuploadsize` (default 100 MB) are rejected with HTTP 413; 1.7 had no limit. There is no setting for unlimited uploads, so set `maxuploadsize` to a large enough value if larger files are uploaded.
- **Connection pooling:** in 1.7, `database` was never pooled, and `queryDatabase` only if both `connectionPoolInit` and `connectionPoolMax` were greater than 0. In 2.0, `database` and `queryDatabase` each use a HikariCP pool as soon as their `connectionPoolMax` is greater than 0 (`connectionPoolInit` is optional). To keep the 1.7 behaviour of `database` (no pool, no idle connections), remove `connectionPoolMax` or set it to 0 or less. Without `connectionPoolMax`, no pool is used, which is recommended for deployments with many instances against one PostgreSQL cluster. A warning is logged at startup whenever 2.0 pools connections that 1.7 did not pool, e.g. because of a `connectionPoolMax` in `database` that 1.7 ignored. HikariCP's leak detection is disabled, since exports and loads hold their connections for minutes.
- **Connection budgets:** requests run on virtual threads and are no longer limited by the thread pool of the servlet container; instead, 2.0 limits the database connections of an instance per database: `maxQueryConnections` (default 10) for short operations of requests, `maxExportConnections` (default 2) for streamed exports and `maxetlprocesses` (default 5) for loads, i.e. at most 17 connections per instance and database with the defaults (e.g. 1360 for 80 instances against one cluster). A request that finds no free connection waits up to `connectionWaitTimeout` (default `querytimeout` if set, otherwise 30 seconds) and then gets HTTP 503. Check the sum over all instances against `max_connections` of the cluster and lower the limits, or use PgBouncer (see the README). Values that are not numbers greater than 0 (`queryLockTimeout`: not greater than or equal to 0) prevent the startup.
- **Lock timeout of requests:** short operations of requests run with a PostgreSQL `lock_timeout` of `queryLockTimeout` (default half of `connectionWaitTimeout`). A request for a dataset that is blocked behind a long exclusive lock (e.g. a manual `LOCK TABLE`) gets HTTP 503 after that time instead of waiting without limit as in 1.7; requests for other datasets are not affected. A replace or a column-name cleanup that waits for a long reader (e.g. an export or `pg_dump`) only blocks requests for its dataset for at most about 3 seconds at a time, so these requests are delayed but succeed. With PgBouncer in transaction pooling mode, set `queryLockTimeout` to 0 and configure the lock timeout on the server side (see the README).
- **Query timeout:** `querytimeout` now also limits the reads of requests that precede a query (dataset and alias lookups, column names, row counts), which also answer HTTP 503 when cancelled.
- **Monitoring:** the Actuator `db` health indicator is disabled and replaced by RowStore's own indicator, which checks the database and the query database within the connection budgets and reports `BUSY` (health UP) if all connections are in use and DOWN only if a database is unreachable. `/status` has the additional fields `connections` with the usage of the budgets and `queuedEtlProcesses`; its other fields are unchanged (`datasets` is -1 if no connection became available in time). Responses with HTTP 503 because the database is busy or unavailable carry `Retry-After: 5`; requests that got no connection in time are no longer logged individually at ERROR level (the budget logs aggregated warnings), and lock and statement timeouts are logged at WARN level at most once per minute.
- **Column names of existing datasets:** column names that 1.x stored with a byte order mark or soft hyphens (e.g. `\ufeff"kopare_id"` instead of `kopare_id`) are renamed automatically in the background after the first start, one dataset at a time, to the names a fresh import of the same CSV file yields. A dataset is skipped with a WARN and left unchanged if a cleaned name would be empty or collide with another column name, or if a row contains both an old and its cleaned name. An append that arrives first renames the columns itself. Queries keep working during the rename (only its end takes the same exclusive lock as a replace: brief, unless a long export or backup holds the table, in which case the rename waits like a replace); afterwards, queries, `/info`, Swagger and exports use the cleaned names, and queries by the old names stop working. Configurations that stored the old names (e.g. in EntryScape) must be updated; each rename is logged at INFO level with the old and the new names. The rename rewrites all rows of an affected dataset including its indexes, so until autovacuum has run, the database needs about twice the dataset's size and the write-ahead log grows accordingly (a read replica may lag behind). While a dataset is renamed, it occupies one of the `maxetlprocesses` slots and uploads to that dataset wait; with many affected datasets, the cleanup after the first start of 2.0 can take a long time. Only affected datasets are rewritten. Once a run has checked all datasets without failures, it is recorded in the new table `maintenance_tasks`, and later starts skip the check; deleting its row (`column-name-cleanup`) makes it run again, e.g. after restoring data tables of 1.x. New uploads always yield cleaned names.
- **Empty lines in CSV files:** an empty line between rows (or directly after the header) now fails the load (status 4, "Error"); 1.7 silently dropped all following rows (default parser) or loaded an empty row (legacy parser). Trailing empty lines are ignored, whitespace-only lines are rows, and an empty value in a single-column CSV must be quoted (`""`). As in 1.7, a failed upload's status is overwritten by the next successful upload of the same dataset, so clients that upload a dataset as several files must check the status after each upload.
- **Replace (PUT):** the previous data is served until the replace is complete and is kept if the replace fails (status 4 with the previous data). The database needs disk space for the old and the new data during a replace. Like 1.x, the replace waits for long-running readers of the dataset's table (e.g. an export or `pg_dump`); unlike 1.x, it tries to lock the table for at most 3 seconds at a time and retries with growing delays (up to 30 seconds), so that new queries of the dataset are delayed by at most about 3 seconds at a time instead of waiting behind the replace, and it gives up after about 30 minutes (status 4 with the previous data). Behind readers that overlap continuously, a replace can therefore fail where 1.x would eventually have succeeded. The sessions holding the table are logged. A replace drops the old table and only carries over its owner and table privileges: objects added to data tables outside of RowStore (manually created indexes, triggers, comments, storage parameters, column privileges, publication membership) are lost with the first replace, and views on a data table make every replace of the dataset fail. If a replace fails after it has discarded the dataset's waiting uploads (see "Upload order"), the dataset keeps the data from before those uploads.
- **Upload order:** uploads to the same dataset are processed one after another in the order of arrival; a replace discards the dataset's uploads that are still waiting. `activeEtlProcesses` in `/status` counts only running loads; the new field `queuedEtlProcesses` counts the loads that are waiting. Datasets left in status 2 ("Processing") by a crash, and datasets in status 1 ("Accepted data") whose upload was still queued when RowStore stopped (the queue is kept in memory), are set to 4 ("Error") at startup; the previous data is still served, and the upload has to be repeated.
- **Database outages:** requests for a dataset (`/dataset/{id}` and its sub-resources, including aliases) that cannot reach the database get HTTP 503 instead of 404; other database errors while looking up a dataset give HTTP 500 instead of 404. An export that gets no export connection within `connectionWaitTimeout` gets HTTP 503 before streaming.
- **Database at startup:** if the database is not reachable at startup (e.g. during a restart or failover of the cluster), RowStore waits for it: the schema migration and the startup maintenance are retried with a delay of 1 second, doubled up to 60 seconds, without limit, and each attempt is logged at WARN level. Meanwhile RowStore does not listen on its port, so health checks fail to connect. 1.7 started without the database and recovered once it was reachable. Other errors, e.g. wrong credentials or a failing migration, end the startup.
- **Export:** CSV exports have one cell per header column, in header order. Rows with duplicate values keep all their cells (1.7 dropped repeated values and shifted the following columns to the left), and rows with fewer cells than the header get empty cells (1.7 silently dropped such rows and could end the export early). If the rows of a dataset, or for CSV its column names, cannot be read, the export returns HTTP 500 instead of 200 with an empty or partial body. A database failure after streaming has started aborts the connection, so clients see an incomplete transfer instead of a body that looks complete (1.7 always ended a JSON export with `]`). Like 1.7, exports have no overall time limit.
- **Per-client rate limit:** the client address is the last `X-Forwarded-For` entry, which the reverse proxy appends or sets; entries further left are ignored, so clients can no longer choose their rate limit key (1.7 used the whole header as written by the client). This works without configuration for a proxy on any address, since RowStore must only be reachable through it. `trustedproxies` (new) restricts `X-Forwarded-For` to listed proxies, e.g. for several proxies in a chain, and `[]` ignores it.
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
