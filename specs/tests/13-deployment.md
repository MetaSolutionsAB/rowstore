# Deployment Tests

## Overview

Tests for the command line and configuration location checks at startup ([CFG-1.03](../06-configuration.md#cfg-1-file-format), [CFG-2.02](../06-configuration.md#cfg-2-resolution-order), [CFG-7.05](../06-configuration.md#cfg-7-cli-options)) and for the access log ([DEPL-6.02](../08-deployment.md#depl-6-logging), [CFG-3.12](../06-configuration.md#cfg-3-application-options)). The shutdown of the ETL processor ([DEPL-7.01](../08-deployment.md#depl-7-shutdown)) is covered by TC-ETL-070 and TC-ETL-071 in [11-etl-processing.md](11-etl-processing.md).

- TC-DEPL-001 to TC-DEPL-009 are unit tests (`RowStoreApplicationTest`, `RowStoreConfigLoaderTest`, `AccessLogFilterTest`); they need neither Docker nor a running server.
- TC-DEPL-010 and TC-DEPL-011 are integration tests (`AccessLogIT`) that read the log of the in-process server; they are skipped against an external instance.

## Test Cases

### TC-DEPL-001: Options of 1.x are rejected with their replacement

**Input:** `-c`, `--config=...`, `--config`, `-p`, `--port=9000`, `-l`, `--log-level=DEBUG`, `--connector-params=a=b`, `-h` (each after a valid `--server.port=8282`)

**Assertions:**
1. The check returns a message that names the argument, says that the options of 1.x were removed and names the replacement (`--rowstore.config.uri=<URI>`, `--server.port=<PORT>`, `loglevel`, `maxQueryConnections`, README)

### TC-DEPL-002: Arguments that are not of the form --name=value are rejected

**Input:** `9000`, `file:///etc/rowstore.json`, `-x`, `--`, `rowstore.json`

**Assertions:**
1. The check returns a message that names the argument and the expected form `--name=value`

### TC-DEPL-003: Spring Boot options are accepted

**Input:** no arguments; `--rowstore.config.uri=...`, `--server.port=9000`, `--debug`, `--server.servlet.context-path=/rowstore`

**Assertions:**
1. The check passes (no message)

### TC-DEPL-004: Absolute URIs are used as they are, file paths are resolved against the working directory

**Input:** `file:///etc/rowstore/rowstore.json`, `https://example.com/rowstore.json`, `/etc/rowstore/rowstore.json`, `conf/rowstore.json`, `./my config.json`

**Assertions:**
1. The URIs are unchanged
2. The paths become `file:` URIs; relative paths are resolved against the working directory, also with spaces

### TC-DEPL-005: A missing, unreadable or invalid configuration is reported without a stack trace

**Input:** a path to a missing file, a file with invalid JSON, a file without `database`

**Assertions:**
1. Each fails with `RowStoreConfigException` and a message naming the problem: `Unable to read the configuration from <URI>: ...`, `rowstore.json invalid: ...`, `rowstore.json: missing required key 'database'`
2. `RowStoreConfigFailureAnalyzer` reports a bean creation failure caused by it with the message of the cause as description and an action

### TC-DEPL-006: A request is logged in the format of 1.x with the client address from X-Forwarded-For

**Input:** `GET /dataset/abc?name=Bo&_limit=10` with `X-Forwarded-For: 6.6.6.6, 203.0.113.7`, `User-Agent`, `Referer` and `Host: rowstore.example.com`; response status 404

**Assertions:**
1. One line `203.0.113.7 "GET /dataset/abc name=Bo&_limit=10" 404  0 http://rowstore.example.com curl/8.5.0 https://example.com/page` (the response size of a mock response is unknown, hence empty)

### TC-DEPL-007: Missing query, headers and body give empty fields, a request body its size

**Input:** `GET /status` without query, user agent and referrer; `POST /datasets` with an 8-byte body; `POST /datasets` with `Transfer-Encoding: chunked`

**Assertions:**
1. Empty fields for the query, the user agent and the referrer
2. Request bytes `0`, `8` and `-`

### TC-DEPL-008: An asynchronous response is logged when it is complete

**Steps:**
1. A request whose handler starts async processing passes the filter
2. The async context is completed

**Assertions:**
1. Nothing is logged after step 1
2. One line with the final status is logged after step 2

### TC-DEPL-009: A failing request is logged with 500, a disabled access log logs nothing

**Steps:**
1. A request whose handler throws a `ServletException` passes the filter
2. A request passes a filter created with `"accesslog": false`

**Assertions:**
1. The exception propagates and one line with status 500 is logged
2. Nothing is logged for step 2

### TC-DEPL-010: Requests are logged with the client address, status and the size of the response body

**Steps:**
1. `GET /status` with `X-Forwarded-For: 6.6.6.6, 203.0.113.7` and `User-Agent: access-log-test`
2. `GET /status;x` with `X-Forwarded-For: 203.0.113.7` (rejected by `PathParameterFilter` before `TrustedProxyFilter`)

**Assertions:**
1. A line `203.0.113.7 "GET /status " 200 <size of the body> 0 <base URL> access-log-test ` is logged
2. A line starting with `203.0.113.7 "GET /status;x " 400 ` is logged

### TC-DEPL-011: A streamed export is logged when it is complete, with the size of the streamed body

**Steps:**
1. Create a dataset and wait until it is available
2. Export it as CSV with `X-Forwarded-For: 203.0.113.7`

**Assertions:**
1. A line starting with `203.0.113.7 "GET /dataset/<id>/export " 200 <size of the exported body> 0 ` is logged
