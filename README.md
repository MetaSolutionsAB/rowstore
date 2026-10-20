# RowStore

RowStore is a minimum footprint storage and query engine for tabular data with two main purposes:

1. A pipeline to transform tabular data (CSV) to JSON and to load the transformed data into a JSON-optimized store (PostgreSQL), and
2. A query interface to fetch all rows that match certain column values.

RowStore 2.0 is a Spring Boot application that is distributed as a single executable JAR (Java 25) and as a Docker image.

## Quickstart

Requirements: Java 25, Maven 3.9+ and Docker with the Compose plugin. All commands are run from the root of this repository.

1. Start PostgreSQL 16 (defined in `docker-compose.yml`, data in the named volume `rowstore-pgdata`, listening on `localhost:15432`):

   ```
   docker compose up -d
   ```

2. Build RowStore:

   ```
   mvn -Dmaven.test.skip=true install
   ```

3. Start RowStore with the example configuration `rowstore.example.json`, which matches the database of `docker-compose.yml` (the version in the JAR name is the one in `VERSION.txt`, e.g. `2.0-SNAPSHOT`):

   ```
   java -jar target/rowstore-*.jar --rowstore.config.uri=file://$PWD/rowstore.example.json
   ```

   RowStore listens on port 8282 and creates its tables at the first start.

4. In another terminal, load a CSV file, wait until the dataset is available (status 3) and query it:

   ```
   printf 'name,city\nAnna,Uppsala\nBo,Lund\n' > example.csv
   curl -i -X POST -H 'Content-Type: text/csv' --data-binary @example.csv http://localhost:8282/datasets
   # The response contains the dataset URL, e.g. "url": "http://localhost:8282/dataset/<id>"
   curl http://localhost:8282/dataset/<id>/info
   curl 'http://localhost:8282/dataset/<id>?city=Lund'
   ```

5. Stop RowStore with Ctrl+C. `docker compose down` stops PostgreSQL and keeps the data, `docker compose down -v` also deletes the volume.

## Installation

### Building

RowStore is built using Maven:

```
mvn -Dmaven.test.skip=true install
```

The result is the executable JAR `target/rowstore-<version>.jar`. RowStore 2.0 replaces the WAR file and the standalone distribution (`bin/rowstore`) of 1.x.

### Running

```
java -jar rowstore-<version>.jar --rowstore.config.uri=file:///path/to/rowstore.json
```

The configuration file (see [Configuration](#configuration)) is looked up in this order:

1. the `--rowstore.config.uri=<URI>` argument (`file://`, `http://` or `https://` URI),
2. the environment variable `ROWSTORE_CONFIG_URI` (same format), e.g. `ROWSTORE_CONFIG_URI=file:///etc/rowstore/rowstore.json java -jar rowstore-<version>.jar`,
3. a file `rowstore.json` on the classpath.

If no configuration is found or it is invalid, RowStore does not start. RowStore listens on port 8282; use `--server.port=<port>` to change it (other Spring Boot properties can be passed the same way). At startup, RowStore creates or migrates its tables with Flyway; an existing 1.x database is adopted as it is. When upgrading from 1.x, read the upgrade notes of version 2.0 in `CHANGELOG.md`.

Besides the REST API, the same port serves the Spring Boot Actuator endpoints (`/actuator/health`, `/actuator/info`, `/actuator/metrics`), the OpenAPI documentation (`/v3/api-docs`, `/swagger-ui.html`) and JVM details (`/status?jvm`). They are meant for operators only and must be blocked by the reverse proxy, see [Security](#security).

### Docker

The Docker image `metasolutions/rowstore:<version>` (Docker Hub) is built from the `Dockerfile` in this repository and reads its configuration from `/srv/rowstore/rowstore.json`:

```
docker run -p 8282:8282 -v /path/to/rowstore.json:/srv/rowstore/rowstore.json metasolutions/rowstore:<version>
```

To build the image yourself, build the JAR first (the `Dockerfile` copies `target/rowstore-*.jar`):

```
mvn -Dmaven.test.skip=true install
docker build -t rowstore .
```

Note that `localhost` in the configuration refers to the container itself; use a host name or address under which the container can reach PostgreSQL.

## CSV conventions

In order for RowStore to be able to properly process CSV files, some conventions have to be followed:

- String values within the tabular data model (such as column titles or cell string values) must contain only Unicode characters. The character encoding is detected automatically.
- The first row should contain short names for each column; they are used as property names during the CSV2JSON conversion. They are also used as variable names by the query API. Upon import, byte order marks (U+FEFF) and soft hyphens (U+00AD) are removed from the column titles, which are then trimmed and converted to lower case. Columns with an empty title are skipped.
- A byte order mark at the beginning of the file is skipped before the file is parsed, so it does not become part of the first column title.
- Datasets loaded by RowStore 1.x whose column names contain byte order marks or soft hyphens (e.g. `\ufeff"kopare_id"` instead of `kopare_id`) are renamed accordingly in the background after the first start of RowStore 2.0, one dataset at a time. Datasets where a cleaned name would be empty or collide with another column name are skipped and logged; they keep their names until they are replaced. After the rename, such columns can only be queried by their cleaned names.
- Comma ("`,`") must be used as column delimiter. There is some basic detection for CSV-files that are using semi-colon ("`;`") as separator, but it is recommended to use comma.
- Quotation marks ("`"`") must be used as quotation characters; a quotation mark within a quoted value is written as two quotation marks (`""`). With `legacyparser` enabled, backslash ("`\`") is the escape character.
- Line feed ("`\n`") or carriage return followed by line feed ("`\r\n`") must be used to indicate a new line (i.e., a new row).
- Empty lines are only allowed at the end of the file. An empty line between rows, including directly after the header, makes the load fail (the dataset status becomes 4, "Error"). A line consisting of whitespace only is a row, and empty lines within a quoted value are part of the value. Consequently, an empty value in a CSV file with a single column must be quoted (`""`).

## REST API

The API consists of a status resource and resources for handling datasets.

With a few exceptions, all resources respond with JSON. CSV uploads must be sent with `Content-Type: text/csv`, alias updates with `Content-Type: application/json`.

### /datasets

- `GET http://{base-url}/datasets` - Returns an array with all dataset ids.
- `POST http://{base-url}/datasets` - Starts the ETL process for a CSV file. Expects a CSV-file and returns HTTP 202 with location of created dataset. Uploads larger than `maxuploadsize` are rejected with HTTP 413.

### /dataset/{id}

- `GET http://{base-url}/dataset/{id}[?column1=value1&column2=value2&_limit=100&_offset=0]` - Queries the dataset with column/value-tuples, see subsection "Querying" below. Browsers requesting HTML are redirected to the web interface (`/dataset/{id}/html`).
- `PUT http://{base-url}/dataset/{id}` - Replaces existing data, same constraints and parameters apply as for `POST http://{base-url}/datasets`. The previous data is served until the new data has been loaded completely and is kept if the load fails.
- `POST http://{base-url}/dataset/{id}` - Adds data to an existing dataset. The column names must match the existing ones.
- `DELETE http://{base-url}/dataset/{id}` - Deletes the dataset (HTTP 204). Returns HTTP 423 while the dataset is being loaded.

Uploads to the same dataset are processed one after another in the order in which they were received. A replace (`PUT`) supersedes uploads to the same dataset that are still waiting to be processed; they are discarded.

An alias may be used in the URL instead of the ID above, see below for handling of aliases.

#### Querying

If no tuples are supplied the whole dataset is returned. Tuple values may be regular expressions if the RowStore instance is configured accordingly (see "Configuration" section below).

Results are paginated. Pagination is controlled through the URL parameters `_limit` and `_offset`: `_limit` accepts values from 1 to the configured `querymaxlimit`, otherwise the default of 100 rows is used. The result object contains `limit`, `offset` and `resultCount`, as well as a `results` array with one JSON object per row, and `prev`/`next` links.

The query engine tries to optimize queries by detecting whether a query value contains any characters that are typical for regular expressions. Queries for partial strings may be affected by undesired optimization, in such cases a regexp query can be enforced by prefixing the query value with `~`. E.g. a query for `name=meta` would not trigger a regexp query, whereas a query for `name=^meta` would. To accept `meta` as regexp it must be prefixed with `~`: `name=~meta`.

Queries are subject to an eventually configured query timeout, see configuration section. If a query exceeds the configured timeout the running request to the database is interrupted and HTTP status 503 is returned. HTTP 503 is also returned if the database is not reachable.

### /dataset/{id}/info

- `GET http://{base-url}/dataset/{id}/info` - Returns information (e.g. status) about a dataset.

Example information object:

```
{
  "status": 3,
  "created": "2026-10-20T23:37:49+0200",
  "columnnames": ["station", "lat", "long", "air quality"],
  "rowcount": 342,
  "identifier": "602e48a4-1e99-45f1-acb9-01d8babd2063",
  "aliases": ["alias1", "alias2"],
  "@context": "https://entrystore.org/rowstore/",
  "@id": "http://localhost:8282/dataset/602e48a4-1e99-45f1-acb9-01d8babd2063"
}
```

Available status values:

- 0: Created
- 1: Accepted data
- 2: Processing
- 3: Available
- 4: Error

A dataset whose replace (`PUT`) failed has status 4 but still serves its previous data.

### /dataset/{id}/aliases

- `GET http://{base-url}/dataset/{id}/aliases` - Returns a JSON array with all aliases of a dataset. This information is also included in the info-object (see above for information about the info-resource).
- `PUT http://{base-url}/dataset/{id}/aliases` - Sets (and replaces if applicable) aliases of the dataset, expects a JSON array.
- `POST http://{base-url}/dataset/{id}/aliases` - Adds an alias, does not replace existing aliases.
- `DELETE http://{base-url}/dataset/{id}/aliases` - Removes all aliases of a dataset.

Aliases may only contain alpha-numeric characters (including Unicode) and may be used as a replacement for the dataset's ID when requesting data. Aliases are basically "nice names" for a dataset.

### Further dataset resources

- `GET http://{base-url}/dataset/{id}/export` - Exports all rows as JSON (`Accept: application/json`) or CSV (`Accept: text/csv`).
- `GET http://{base-url}/dataset/{id}/swagger` - OpenAPI description of the dataset's query interface.
- `GET http://{base-url}/dataset/{id}/html` - Web interface for browsing the dataset.

### /status

- `GET http://{base-url}/status` - Returns some basic information about the RowStore instance: version, number of datasets, and the number of loads that are currently running (`activeEtlProcesses`; uploads waiting for their turn are not counted).

## Configuration

RowStore is configured through a JSON file, see [Running](#running) for how it is located. `rowstore.example.json` is a minimal example for the [Quickstart](#quickstart), and a complete example follows below.

The configuration is validated at startup. If it is invalid, RowStore logs an error naming the offending key (e.g. `rowstore.json: missing required key 'database.host'`) and does not start. Startup fails if:

- a required key is missing: `baseurl`, `database` with `host`, `database`, `user` and `password` (also in `queryDatabase`, if present); an empty password is allowed,
- `baseurl` or one of the required database keys is not a string,
- `database`, `queryDatabase`, `ratelimit` or `cors` is not a JSON object, or `cors.allowedorigins` is not an array of strings,
- `trustedproxies` is not an array of strings or contains an entry that is not an IP address or CIDR range,
- `querymaxlimit` or `maxuploadsize` is not greater than 0.

Unknown keys are ignored (e.g. `exportpagesize`, which was never used and has been removed). Optional numeric and boolean values of the wrong type fall back to their defaults.

### Properties

- `baseurl` (String, required) - The base URL under which the root of RowStore can be reached. Used for generating correct URIs in API responses.
- `regexpqueries` (String) - Determines whether the query interface should allow regular expressions to match column values: `disabled` or `false` (no regexp support, default), `simple` (support for queries starting with `^`), and `full` or `true` (support for any regexp queries).
- `maxetlprocesses` (Integer) - Maximum number of concurrently running loads (ETL processes) across all datasets. Default: 5.
- `legacyparser` (Boolean) - Use OpenCSV's legacy `CSVParser` (backslash as escape character) instead of the RFC 4180 parser. Default: `false`.
- `maxuploadsize` (Integer) - Maximum size of a CSV upload in bytes. Larger uploads are rejected with HTTP 413. Default: 104857600 (100 MB).
- `querytimeout` (Integer) - Query timeout for dataset queries in seconds. By default (or if the value is 0 or less) no query timeout is active, unless configured directly in the database.
- `querymaxlimit` (Integer) - The maximum allowed value of the `_limit` URL parameter when querying a dataset. Must be greater than 0. Default: 100.
- `loglevel` (String) - Log level of RowStore's log output (console): `DEBUG`, `INFO`, `WARN` or `ERROR` (case-insensitive). Default: `INFO`.
- `cors` - Configures CORS.
  - `allowedorigins` (Array of Strings) - Origins that may call RowStore from browsers, e.g. `["https://app.example.com"]`. Default: `["*"]` (all origins), see [Security](#security).
- `database` (required) - Configures the database connection. By default, no connection pool is used (connections are opened and closed per operation with `PGSimpleDataSource`). Set `connectionPoolMax` to a positive integer to enable HikariCP connection pooling.
  - `type` - DB type, currently only `postgresql` is supported. Default: `postgresql`.
  - `host` (required) - Hostname.
  - `port` - Port. Default: `5432`.
  - `database` (required) - Name of database.
  - `user` (required) - Username.
  - `password` (required) - Password, may be empty.
  - `ssl` - `true` or `false`. `true` requires SSL (`sslmode=require`), `false` disables it (`sslmode=disable`), so a server that requires SSL refuses the connection. Default: `false`.
  - `connectionPoolMax` - Maximum size of the connection pool. Set to a positive integer to enable HikariCP pooling. When not set or <= 0, no pool is used. Default: -1 (no pool).
  - `connectionPoolInit` - Minimum idle connections in the pool. Only effective when `connectionPoolMax` > 0. Default: 0.
  - `socketTimeout` - Socket timeout of the database driver in seconds, 0 means unlimited. Default: 0.
  - `connectTimeout` - Connection timeout in seconds, 0 means unlimited. With a connection pool, it also limits the wait for a free pooled connection. Default: 5.
  - `loginTimeout` - Login timeout in seconds, 0 means unlimited. Default: 5.
- `queryDatabase` - Configures the database connection for read-only requests, e.g. if queries should be run against a read replica. Same configuration properties as `database`, including the required ones. Its connections are read-only. Default: the `database` connection.
- `ratelimit` - Configures rate limitation. It is active if `timerange` is greater than 0 and `global` or `dataset` is greater than 0; `/status` is not rate limited. Rejected requests get HTTP 429.
  - `type` - `average` or `slidingwindow` (default).
  - `timerange` - The size (in seconds) of the time slot or window to be used for calculating the limitation.
  - `dataset` - Amount of permitted requests per dataset.
  - `global` - Amount of permitted requests globally for a RowStore instance.
  - `clientip` - Amount of permitted requests per client IP. The client IP is the address of the connecting peer. Only if the peer is a trusted proxy (see `trustedproxies`), the client IP is taken from the `X-Forwarded-For` header: the header is read from right to left, skipping trusted proxies, and the first address that is not a trusted proxy is used. Entries further left can be forged by clients and are ignored. Proxies must write bare addresses: entries with a port (`192.0.2.1:4711`) count as untrusted. If `X-Forwarded-For` arrives from a peer that is not a trusted proxy, a warning is logged (at most once per hour per address).
- `trustedproxies` (Array of Strings) - IP addresses and CIDR ranges of the reverse proxies whose `X-Forwarded-For` header is trusted, e.g. `["10.0.0.5"]`, `["172.18.0.0/16", "fd00::/8"]`. Only IP addresses are accepted, no host names. Default (if not set): loopback, private, shared and link-local ranges (`127.0.0.0/8`, `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `169.254.0.0/16`, `100.64.0.0/10`, `::1`, `fc00::/7`, `fe80::/10`), which fits a reverse proxy on the same host or on a private/Docker network. A reverse proxy with a public address must be listed explicitly. An empty array `[]` trusts nobody, i.e. `X-Forwarded-For` is always ignored and all requests through the proxy share the proxy's per-client limit. If clients can reach RowStore directly from a private address, list exactly the reverse proxy addresses, otherwise such clients can choose their own client IP.

### Example

```
{
  "baseurl": "https://domain.com/rowstore/",
  "regexpqueries": "full",
  "maxetlprocesses": 5,
  "querytimeout": 10,
  "querymaxlimit": 250,
  "maxuploadsize": 209715200,
  "cors": {
    "allowedorigins": ["https://app.example.com"]
  },
  "database": {
    "type": "postgresql",
    "host": "localhost",
    "database": "rowstore",
    "user": "rowstore",
    "password": "",
    "ssl": true
  },
  "queryDatabase": {
    "type": "postgresql",
    "host": "read-replica",
    "database": "rowstore",
    "user": "rowstore",
    "password": "",
    "ssl": false,
    "connectionPoolInit": 5,
    "connectionPoolMax": 10
  },
  "ratelimit": {
    "type": "slidingwindow",
    "timerange": 60,
    "dataset": 30,
    "global": 300,
    "clientip": 60
  },
  "trustedproxies": ["10.0.0.5"],
  "loglevel": "debug"
}
```

## Data loading

A CSV upload is loaded asynchronously in a single database transaction; the dataset's status (see `/dataset/{id}/info`) shows the progress. An append (`POST`) inserts the rows into the dataset's table. A replace (`PUT`) loads the rows into a new table and swaps it for the dataset's table at the end of the same transaction, so queries and exports serve the previous data until the replace is complete, and a failed replace keeps the previous data. During a replace, the database needs disk space for both the old and the new data. The swap has to wait for running readers of the table, e.g. a long export or a backup with `pg_dump`; meanwhile new queries of the dataset wait as well.

## Connection Pooling with PgBouncer

When running many RowStore instances against a shared PostgreSQL cluster, a connection pooler such as [PgBouncer](https://www.pgbouncer.org/) is recommended to avoid connection exhaustion.

### When to use PgBouncer

By default, RowStore uses `PGSimpleDataSource` (no connection pool), which opens and closes connections per operation. This is appropriate for most deployments. However, when dozens or hundreds of instances share a single PostgreSQL cluster, even short-lived connections can cause contention on the server side.

PgBouncer in `transaction` mode multiplexes many application connections over a smaller number of PostgreSQL server connections, reducing load on the database.

### Recommended PgBouncer configuration

```ini
[databases]
rowstore = host=postgres-main port=5432 dbname=rowstore

[pgbouncer]
pool_mode = transaction
default_pool_size = 20
max_client_conn = 1000
```

### RowStore configuration with PgBouncer

Point RowStore at PgBouncer instead of PostgreSQL directly. Connection pooling in RowStore itself is typically not needed when using PgBouncer:

```json
{
  "database": {
    "host": "pgbouncer-host",
    "port": 6432,
    "database": "rowstore",
    "user": "rowstore",
    "password": "secret"
  }
}
```

To enable RowStore-side connection pooling in addition to PgBouncer (useful for high-traffic instances):

```json
{
  "database": {
    "host": "pgbouncer-host",
    "port": 6432,
    "database": "rowstore",
    "user": "rowstore",
    "password": "secret",
    "connectionPoolMax": 5
  }
}
```

**Note:** A CSV import holds a single transaction for the whole import, including the index creation and, for a replace, the swap of the tables (see [Data loading](#data-loading)). In PgBouncer's `transaction` mode, it therefore pins a server connection for the whole duration of the import, as in `session` mode; the same applies to a streamed export. Size `default_pool_size` for the number of concurrent imports (up to `maxetlprocesses` per instance) and exports plus the query load.

## Testing

Tests are run with Maven from the root of the repository.

### Integration tests

Integration tests (`*IT.java`) use Testcontainers to automatically start a disposable PostgreSQL database and an embedded RowStore server. **Requires Docker to be running.**

```
mvn verify
```

`mvn verify` runs the unit tests as well. Run a single test class or method:

```
mvn verify -Dit.test=StatusIT
mvn verify -Dit.test=StatusIT#getStatus_returnsValidStructure
```

To run tests against an external RowStore instance (skips the automatic setup; tests that need direct database access are skipped):

```
mvn verify -Drowstore.baseUrl=http://localhost:8282
```

### Unit tests

Unit tests (`*Test.java`) do not need Docker:

```
mvn test
```

### Test Specifications

Framework-agnostic test specifications are available in `specs/tests/`. These can be used to implement tests in other languages or frameworks.

## Security

RowStore provides a public REST API for data retrieval (`GET` and `HEAD` requests) and a private REST API for data management (all other methods: creating, replacing, appending to and deleting datasets, managing aliases). RowStore has no application-level authentication. It must always run behind a reverse proxy and must never be exposed directly to untrusted clients.

The reverse proxy MUST:

- authenticate all requests with methods other than `GET` and `HEAD`;
- deny public access to `/actuator` and `/actuator/**`, `/swagger-ui.html` and `/swagger-ui/**`, `/v3/api-docs` and `/v3/api-docs/**` (including `/v3/api-docs.yaml`), and `/status?jvm` (any `/status` request with a `jvm` parameter; plain `/status` is public). In RowStore 2.0 these are served on the application port (8282), so blocking them at the proxy is the only protection. RowStore rejects request paths containing `;` with 400 (path parameters would otherwise bypass such path rules, e.g. `/v3;x/api-docs`); the proxy should reject them as well, as defence in depth;
- set `X-Forwarded-For` and be listed in `trustedproxies` (see [Configuration](#properties)), so that the per-client rate limit applies to the real client and cannot be influenced by clients.

CORS: by default all origins are allowed (`*`). This is acceptable only under the contract above: the read API is public anyway, and RowStore never sends `Access-Control-Allow-Credentials`, so web pages of other origins cannot use a browser's proxy credentials to read responses of the private API. To allow only known web applications to call RowStore from browsers, list their origins in `rowstore.json`, e.g. `"cors": { "allowedorigins": ["https://app.example.com"] }`.

A minimal nginx configuration implementing the list above (TLS settings omitted):

```nginx
upstream rowstore {
    server 127.0.0.1:8282;
}

server {
    listen 80;
    server_name rowstore.example.com;

    # Path parameters (";") would bypass the location rules below (e.g. /v3;x/api-docs);
    # RowStore rejects them too, this is defence in depth
    if ($request_uri ~ "^[^?]*;") {
        return 400;
    }

    # Replace any client-supplied value; RowStore must list this proxy in "trustedproxies"
    proxy_set_header X-Forwarded-For $remote_addr;

    # Public read API (GET/HEAD) on /datasets and /dataset/...; all other methods need authentication
    location /dataset {
        limit_except GET HEAD {
            auth_basic "RowStore";
            auth_basic_user_file /etc/nginx/rowstore.htpasswd;
        }
        proxy_pass http://rowstore;
    }

    # /status is public, but /status?jvm exposes JVM details: reject any query string
    # (checking $arg_jvm is not enough, it is empty for "?jvm" and misses encoded names like "?%6Avm")
    location = /status {
        if ($args != "") {
            return 403;
        }
        proxy_pass http://rowstore;
    }

    # Everything else, including /actuator/**, /swagger-ui*, /v3/api-docs*
    location / {
        return 404;
    }
}
```

The example allows only the public API paths and denies everything else. If RowStore listens on another host, set `trustedproxies` to the proxy's address as seen by RowStore. If a load balancer or CDN sits in front of nginx, configure nginx's real IP module (`set_real_ip_from <balancer addresses>; real_ip_header X-Forwarded-For; real_ip_recursive on;`) so that `$remote_addr` is the real client address, and keep `proxy_set_header X-Forwarded-For $remote_addr` instead of `$proxy_add_x_forwarded_for`. Note that `limit_except GET HEAD` also requires authentication for `OPTIONS`, so CORS preflight requests to `/dataset*` (needed only for the private API) get 401; this is acceptable under the contract above.

## Issue tracking

Issues are tracked in the JIRA project [ROWS](https://metasolutions.atlassian.net/browse/ROWS).

## Database layout

The schema of the administrative tables is managed by Flyway (table `flyway_schema_history`, migrations in `src/main/resources/db/migration`).

An administrative table keeps track of datasets and their current status:

`CREATE TABLE IF NOT EXISTS datasets (id UUID PRIMARY KEY, status INT NOT NULL, created TIMESTAMP NOT NULL, data_table CHAR(37))`

A table per dataset (`data_<id without hyphens>`) holds the actual data in JSON:

`CREATE TABLE IF NOT EXISTS {data-table} (rownr BIGSERIAL PRIMARY KEY, data JSONB NOT NULL)`

Data tables created by RowStore 1.x use `rownr SERIAL`; they get `BIGSERIAL` with their next replace.

A table to manage aliases:

`CREATE TABLE IF NOT EXISTS aliases (id SERIAL PRIMARY KEY, dataset_id UUID NOT NULL, alias TEXT NOT NULL)`

## License

[MetaSolutions AB](http://www.metasolutions.se) licenses this work under the terms of the Apache License 2.0 (the "License"); you may not use this work except in compliance with the License. See the `LICENSE.txt` file distributed with this work for the full License.
