# User Stories

## Purpose

Provides scenario-based user stories for all four RowStore personas, illustrating typical workflows from each perspective.

## Scope

Covers data publishers, API consumers, operators/admins, and embedded viewers. Each story includes the persona, goal, preconditions, steps, and expected outcome. For detailed technical specifications, follow the links to the relevant spec files.

## USR-1 Data Publisher Stories

> **USR-1.01 Upload a CSV Dataset**
>
> **Persona**: Data Publisher
> **Goal**: Make a CSV dataset available for querying via the REST API.
> **Preconditions**: A running RowStore instance; a valid CSV file with a header row.
> **Steps**:
> 1. `POST /datasets` with `Content-Type: text/csv` and the CSV file as the request body
> 2. Receive 202 Accepted with the dataset UUID, URL, and info URL
> 3. Poll `GET /dataset/{id}/info` until `status` is `3` (AVAILABLE)
>
> **Expected Outcome**: Dataset is queryable at `GET /dataset/{id}`. Column names match the CSV header row (lowercased).
> **References**: [API-3.03](03-api.md#api-3-endpoint-catalog), [ETL-1.01](04-etl-pipeline.md#etl-1-pipeline-overview)

> **USR-1.02 Update/Replace a Dataset**
>
> **Persona**: Data Publisher
> **Goal**: Replace the contents of an existing dataset with updated CSV data.
> **Preconditions**: An existing dataset in AVAILABLE status.
> **Steps**:
> 1. `PUT /dataset/{id}` with `Content-Type: text/csv` and the new CSV file
> 2. Receive 202 Accepted
> 3. Poll `GET /dataset/{id}/info` until `status` is `3` (AVAILABLE)
>
> **Expected Outcome**: All previous rows are replaced with the new data. Column structure may differ from the original.
> **References**: [API-3.06](03-api.md#api-3-endpoint-catalog), [ETL-6.02](04-etl-pipeline.md#etl-6-append-vs-replace)

> **USR-1.03 Manage Aliases**
>
> **Persona**: Data Publisher
> **Goal**: Assign a human-readable alias to a dataset for easier API access.
> **Preconditions**: An existing dataset.
> **Steps**:
> 1. `POST /dataset/{id}/aliases` with a JSON array of alias strings (e.g., `["citydata"]`)
> 2. Receive 204 No Content
> 3. Verify the dataset is accessible at `GET /dataset/citydata`
>
> **Expected Outcome**: The dataset can be queried using the alias instead of the UUID.
> **References**: [API-3.09](03-api.md#api-3-endpoint-catalog), [SEC-4.02](07-security.md#sec-4-input-validation)

> **USR-1.04 Check Processing Status**
>
> **Persona**: Data Publisher
> **Goal**: Determine whether an uploaded CSV has been processed.
> **Preconditions**: A recently uploaded dataset.
> **Steps**:
> 1. `GET /dataset/{id}/info`
> 2. Check the `status` field: 0=CREATED, 1=ACCEPTED_DATA, 2=PROCESSING, 3=AVAILABLE, 4=ERROR
>
> **Expected Outcome**: Status indicates the current processing phase.
> **References**: [API-3.08](03-api.md#api-3-endpoint-catalog), [DOM-3.01](02-domain-model.md#dom-3-etl-status-lifecycle)

> **USR-1.05 Delete a Dataset**
>
> **Persona**: Data Publisher
> **Goal**: Remove a dataset and all its data.
> **Preconditions**: An existing dataset in AVAILABLE or ERROR status.
> **Steps**:
> 1. `DELETE /dataset/{id}`
> 2. Receive 204 No Content
>
> **Expected Outcome**: Dataset, its data table, and all aliases are permanently removed. Returns 423 Locked if dataset is still processing.
> **References**: [API-3.07](03-api.md#api-3-endpoint-catalog)

## USR-2 API Consumer Stories

> **USR-2.01 Discover Datasets**
>
> **Persona**: API Consumer
> **Goal**: Find available datasets.
> **Preconditions**: A running RowStore instance.
> **Steps**:
> 1. `GET /datasets`
> 2. Receive a JSON array of dataset UUIDs
> 3. For each UUID, `GET /dataset/{id}/info` to inspect metadata
>
> **Expected Outcome**: A list of all available dataset IDs with their metadata.
> **References**: [API-3.02](03-api.md#api-3-endpoint-catalog), [API-3.08](03-api.md#api-3-endpoint-catalog)

> **USR-2.02 Query with Column Filters**
>
> **Persona**: API Consumer
> **Goal**: Retrieve rows matching specific criteria.
> **Preconditions**: A dataset in AVAILABLE status.
> **Steps**:
> 1. `GET /dataset/{id}/info` to discover column names
> 2. `GET /dataset/{id}?city=Stockholm&age=25`
> 3. Receive JSON response with matching rows
>
> **Expected Outcome**: Response envelope with filtered `results`, `resultCount`, `limit`, `offset`, and `queryTime`.
> **References**: [API-3.04](03-api.md#api-3-endpoint-catalog), [QUERY-2](05-querying.md#query-2-filter-mechanics)

> **USR-2.03 Paginate Results**
>
> **Persona**: API Consumer
> **Goal**: Iterate through a large result set page by page.
> **Preconditions**: A dataset with more rows than the default limit.
> **Steps**:
> 1. `GET /dataset/{id}?_limit=50&_offset=0`
> 2. Follow the `next` URL in the response to get the next page
> 3. Repeat until `next` is null
>
> **Expected Outcome**: All matching rows retrieved across multiple pages.
> **References**: [QUERY-3.01](05-querying.md#query-3-special-parameters), [QUERY-6](05-querying.md#query-6-pagination)

> **USR-2.04 Use Regex Filters**
>
> **Persona**: API Consumer
> **Goal**: Find rows matching a pattern (requires `regexpqueries` enabled).
> **Preconditions**: A dataset in AVAILABLE status; server configured with `regexpqueries: "full"`.
> **Steps**:
> 1. `GET /dataset/{id}?name=^Stoc` — finds names starting with "Stoc"
> 2. `GET /dataset/{id}?name=~^[A-Z]` — explicit regex prefix
>
> **Expected Outcome**: Rows matching the regex pattern are returned.
> **References**: [QUERY-4](05-querying.md#query-4-regex-modes)

> **USR-2.05 Export Full Dataset**
>
> **Persona**: API Consumer
> **Goal**: Download the entire dataset.
> **Preconditions**: A dataset in AVAILABLE status.
> **Steps**:
> 1. `GET /dataset/{id}/export` with `Accept: application/json` for JSON export
> 2. Or `GET /dataset/{id}/export` with `Accept: text/csv` for CSV export
>
> **Expected Outcome**: Complete dataset streamed as a download with `Content-Disposition` header.
> **References**: [API-3.10](03-api.md#api-3-endpoint-catalog)

> **USR-2.06** [REMOVED] Use JSONP for Cross-Origin Access — JSONP support has been removed. See [USR-2.08](#usr-2-api-consumer-stories) for CORS-based cross-origin access.

> **USR-2.07 Get OpenAPI Spec for a Dataset**
>
> **Persona**: API Consumer
> **Goal**: Generate client code or explore the API for a specific dataset.
> **Preconditions**: A dataset in AVAILABLE status.
> **Steps**:
> 1. `GET /dataset/{id}/swagger`
> 2. Load the returned OpenAPI JSON into Swagger UI or a code generator
>
> **Expected Outcome**: A valid OpenAPI specification with query parameters matching the dataset's column names.
> **References**: [API-3.11](03-api.md#api-3-endpoint-catalog)

> **USR-2.08 Use CORS for Cross-Origin Access**
>
> **Persona**: API Consumer
> **Goal**: Access RowStore data from a browser on a different domain.
> **Preconditions**: A dataset in AVAILABLE status; CORS allowed origins configured (default: all origins).
> **Steps**:
> 1. Make a `fetch()` request from JavaScript on a different origin:
>    ```javascript
>    const response = await fetch('https://rowstore.example.com/dataset/{id}?city=Stockholm');
>    const data = await response.json();
>    ```
> 2. The browser automatically sends `Origin` header and validates the CORS response
>
> **Expected Outcome**: Data is accessible from any allowed origin. The server includes `Access-Control-Allow-Origin` in the response.
> **References**: [SEC-6.01](07-security.md#sec-6-cors), [CFG-3.10](06-configuration.md#cfg-3-application-options)

## USR-3 Operator/Admin Stories

> **USR-3.01 Deploy a Standalone Instance**
>
> **Persona**: Operator/Admin
> **Goal**: Set up a new RowStore instance.
> **Preconditions**: Java 25, PostgreSQL 9.4+.
> **Steps**:
> 1. Create a PostgreSQL database
> 2. Write a `rowstore.json` config file (see [CFG-9.01](06-configuration.md#cfg-9-annotated-example))
> 3. Build: `mvn -Dmaven.test.skip=true install`
> 4. Run: `java -jar target/rowstore-2.0-SNAPSHOT.jar --rowstore.config.uri=file:///path/to/rowstore.json`
> 5. Verify: `GET /status`
>
> **Expected Outcome**: RowStore is running and responding to requests. Schema is auto-created by Flyway.
> **References**: [DEPL-1](08-deployment.md#depl-1-spring-boot-standalone), [CFG-9](06-configuration.md#cfg-9-annotated-example)

> **USR-3.02 Configure Rate Limiting**
>
> **Persona**: Operator/Admin
> **Goal**: Protect the instance from excessive query traffic.
> **Preconditions**: A running RowStore instance.
> **Steps**:
> 1. Add a `ratelimit` section to the config:
>    ```json
>    "ratelimit": {
>        "type": "slidingwindow",
>        "timerange": 60,
>        "global": 1000,
>        "dataset": 100
>    }
>    ```
> 2. Restart RowStore
>
> **Expected Outcome**: Requests exceeding the limit receive 429 with `Retry-After` header.
> **References**: [CFG-6](06-configuration.md#cfg-6-rate-limit-configuration), [SEC-5](07-security.md#sec-5-rate-limiting)

> **USR-3.03 Monitor Status and JVM Metrics**
>
> **Persona**: Operator/Admin
> **Goal**: Check system health and resource usage.
> **Preconditions**: A running RowStore instance.
> **Steps**:
> 1. `GET /status` — basic health check
> 2. `GET /status?jvm` — memory, processors, heap details
> 3. `GET /actuator/health` — Spring Boot health with database connectivity
> 4. `GET /actuator/metrics` — detailed JVM and connection pool metrics
>
> **Expected Outcome**: Service name, version, dataset count, active ETL processes, and (optionally) JVM memory metrics. Actuator provides additional database and pool metrics.
> **References**: [DEPL-5](08-deployment.md#depl-5-monitoring)

> **USR-3.04 Set Up a Read Replica**
>
> **Persona**: Operator/Admin
> **Goal**: Route read traffic to a PostgreSQL replica for scalability.
> **Preconditions**: A PostgreSQL read replica with the same schema.
> **Steps**:
> 1. Add a `queryDatabase` section to the config with the replica's connection details
> 2. Restart RowStore
>
> **Expected Outcome**: All query requests use the replica connection. Writes continue on the primary.
> **References**: [CFG-5](06-configuration.md#cfg-5-read-replica-configuration), [QUERY-8](05-querying.md#query-8-read-replica-routing)

> **USR-3.05 Configure Query Timeouts**
>
> **Persona**: Operator/Admin
> **Goal**: Prevent long-running queries from consuming database resources.
> **Preconditions**: A running RowStore instance.
> **Steps**:
> 1. Set `"querytimeout": 30` in the config (30 seconds)
> 2. Restart RowStore
>
> **Expected Outcome**: Queries exceeding 30 seconds are cancelled by PostgreSQL and return 503.
> **References**: [CFG-3.06](06-configuration.md#cfg-3-application-options), [QUERY-7](05-querying.md#query-7-query-timeout)

> **USR-3.06 Troubleshoot ETL Errors**
>
> **Persona**: Operator/Admin
> **Goal**: Diagnose why a CSV upload failed.
> **Preconditions**: A dataset with status ERROR (4).
> **Steps**:
> 1. `GET /dataset/{id}/info` — confirm status is 4
> 2. Check server logs for the error details (charset issues, malformed CSV, database errors)
> 3. Fix the CSV and re-upload via `PUT /dataset/{id}`
>
> **Expected Outcome**: Root cause identified in logs. Re-upload succeeds.
> **References**: [DOM-3.01](02-domain-model.md#dom-3-etl-status-lifecycle), [ETL-3.02](04-etl-pipeline.md#etl-3-status-lifecycle)

## USR-4 Embedded Viewer Stories

> **USR-4.01 Browse Dataset in Full-Page GUI**
>
> **Persona**: Embedded Viewer
> **Goal**: Explore a dataset's contents in a browser.
> **Preconditions**: A dataset in AVAILABLE status.
> **Steps**:
> 1. Navigate to `https://rowstore.example.com/dataset/{id}/html`
> 2. Browse the data table with pagination controls
> 3. Use column filters to narrow results
>
> **Expected Outcome**: Full-page GUI with navbar, data table, column filters, and pagination.
> **References**: [UIX-2.01](09-ui-ux.md#uix-2-display-modes), [API-3.12](03-api.md#api-3-endpoint-catalog)

> **USR-4.02 Embed Dataset Table in an Iframe**
>
> **Persona**: Embedded Viewer
> **Goal**: Display a dataset table within an external webpage.
> **Preconditions**: A dataset in AVAILABLE status.
> **Steps**:
> 1. Add an iframe to the external page:
>    ```html
>    <iframe src="https://rowstore.example.com/dataset/{id}/html?embed"
>            width="100%" height="600"></iframe>
>    ```
>
> **Expected Outcome**: Minimal data table without navbar or footer, suitable for embedding.
> **References**: [UIX-2.02](09-ui-ux.md#uix-2-display-modes)

> **USR-4.03 Filter and Paginate in the GUI**
>
> **Persona**: Embedded Viewer
> **Goal**: Find specific data within the web interface.
> **Preconditions**: The GUI is open for a dataset.
> **Steps**:
> 1. Type a filter value in a column's input field
> 2. Press Enter to apply the filter
> 3. Use pagination controls to navigate through filtered results
> 4. Toggle column visibility to focus on relevant columns
>
> **Expected Outcome**: Table displays only matching rows with the selected columns visible.
> **References**: [UIX-3](09-ui-ux.md#uix-3-features)

## Known Limitations

- User stories cover the four identified personas only
- No stories for multi-tenant or authentication scenarios (not yet implemented)
- No stories for concurrent upload/query scenarios

## References

- [REST API](03-api.md#api-3-endpoint-catalog) — All API endpoints
- [ETL Pipeline](04-etl-pipeline.md#etl-1-pipeline-overview) — ETL pipeline
- [Query Processing](05-querying.md#query-1-query-overview) — Query processing
- [Configuration](06-configuration.md#cfg-3-application-options) — Configuration options
- [Security](07-security.md#sec-5-rate-limiting) — Security features
- [Deployment](08-deployment.md#depl-1-spring-boot-standalone) — Deployment procedures
- [Web GUI](09-ui-ux.md#uix-1-overview) — Web GUI features
- [Glossary](12-glossary.md#glo-1-terms) — Term definitions

## Change Log

| Date | Description |
|------|-------------|
| 2026-02-06 | Initial version |
| 2026-02-09 | Updated for Spring Boot migration: JSONP story removed, CORS story added, deployment steps updated, Actuator monitoring added |
