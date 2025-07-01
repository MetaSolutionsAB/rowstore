# ROWSTORE CHANGELOG

## 1.7 (2025-07-01)

### Improvement

[ROWS-93](https://metasolutions.atlassian.net/browse/ROWS-93) Switch to RFC4180Parser by default

[ROWS-94](https://metasolutions.atlassian.net/browse/ROWS-94) Enable logging for PostgreSQL driver

[ROWS-95](https://metasolutions.atlassian.net/browse/ROWS-95) Add settings for DB timeouts and set default values

[ROWS-98](https://metasolutions.atlassian.net/browse/ROWS-98) Bump dependencies to newer versions

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