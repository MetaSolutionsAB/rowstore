# RowStore

RowStore is a minimum footprint storage and query engine for tabular data with two main purposes:

1. A pipeline to transform tabular data (CSV) to JSON and to load the transformed data into a JSON-optimized store (PostgreSQL), and
2. A query interface to fetch all rows that match certain column values.

RowStore 2.0 is a Spring Boot application that is distributed as a single executable JAR (Java 25) and as a Docker image.

## Quickstart

Requirements: Java 25, Maven 3.6.3+ and Docker with the Compose plugin. All commands are run from the root of this repository.

1. Start PostgreSQL 16 (defined in `docker-compose.yml`, data in the named volume `rowstore-pgdata`, which Docker Compose prefixes with the project name, e.g. `rowstore_rowstore-pgdata`; listening on `localhost:15432`):

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

1. the `--rowstore.config.uri=<location>` argument: a `file://`, `http://` or `https://` URI, or a file path (relative paths are resolved against the working directory),
2. the environment variable `ROWSTORE_CONFIG_URI` (same format), e.g. `ROWSTORE_CONFIG_URI=/etc/rowstore/rowstore.json java -jar rowstore-<version>.jar`,
3. a file `rowstore.json` on the classpath.

If no configuration is found, or if it cannot be read or is invalid, RowStore logs one error message naming the problem and does not start (exit code 1). RowStore listens on port 8282; use `--server.port=<port>` to change it (other Spring Boot properties can be passed the same way). RowStore only takes arguments of the form `--name=value`; any other argument, including the options of 1.x (`-c`/`--config`, `-p`/`--port`, `-l`/`--log-level`, `--connector-params`), prevents the startup with a message that names the replacement. The environment variable `ROWSTORE_CONNECTOR_PARAMS` of 1.x is ignored with a warning. At startup, RowStore creates or migrates its tables with Flyway; an existing 1.x database is adopted as it is. When upgrading from 1.x, read the upgrade notes of version 2.0 in `CHANGELOG.md`.

A database must be used by only one RowStore instance at a time. Uploads are queued in memory and loaded one after another per dataset, a dataset that is in status PROCESSING (2) without a load in this instance is taken over, and at startup datasets left in PROCESSING by an interrupted load or in ACCEPTED_DATA (1) by an upload that was still queued when RowStore stopped are set to ERROR (4). A second instance on the same database, including an old and a new instance that overlap during a deployment, could therefore load the same dataset concurrently or reset the loads of the other instance. Stop the old instance before starting the new one. Several instances can share a PostgreSQL cluster if each has its own database. Support for several instances per database is planned ([ROWS-149](https://metasolutions.atlassian.net/browse/ROWS-149)).

Besides the REST API, the same port serves the Spring Boot Actuator endpoints (`/actuator/health`, `/actuator/info`, `/actuator/metrics`), the OpenAPI documentation (`/v3/api-docs`, `/swagger-ui.html`) and JVM details (`/status?jvm`). They are meant for operators only and must be blocked by the reverse proxy, see [Security](#security). `/actuator/health` checks the database and, if configured, the query database: a database is reported as `UP`, as `BUSY` if all its connections are in use (it is reachable, so the health stays UP), or as `DOWN` if it cannot be reached, which makes the health DOWN.

RowStore logs one line per request (access log, logger `org.entrystore.rowstore.LogService`) in the format of 1.x, after the response is complete: `<client address> "<method> <path> <query>" <status> <response bytes> <request bytes> <scheme>://<host> <user agent> <referrer>`, e.g. `203.0.113.7 "GET /dataset/1b4f… name=Bo" 200 1234 0 https://rowstore.example.com curl/8.5.0 `. The client address is resolved from `X-Forwarded-For` like for rate limiting (`trustedproxies`), missing values are empty. Set `"accesslog": false` to turn it off.

#### Shutdown

On SIGTERM (e.g. `docker stop`), RowStore stops accepting requests and lets running requests finish for up to 30 seconds. It then discards the uploads that are still queued and logs their datasets (they have to be uploaded again; their status is set to 4 at the next startup) and lets running loads finish for up to another 30 seconds. A load that is still running after that is rolled back and its dataset set to 4 at the next startup. Allow at least 70 seconds for the shutdown (e.g. `docker stop -t 70`, `stop_grace_period: 70s` in Docker Compose, `terminationGracePeriodSeconds: 70` in Kubernetes); the default of Docker (10 seconds) kills RowStore while requests and loads are still running.

### Docker

The Docker image `metasolutions/rowstore:<version>` (Docker Hub) is built from the `Dockerfile` in this repository and reads its configuration from `/srv/rowstore/rowstore.json` (environment variable `ROWSTORE_CONFIG_URI`):

```
docker run -p 8282:8282 -v /path/to/rowstore.json:/srv/rowstore/rowstore.json --stop-timeout 70 metasolutions/rowstore:<version>
```

- Arguments after the image name are passed to RowStore, e.g. `--server.port=9000`; the configuration is still read from `/srv/rowstore/rowstore.json`.
- JVM options are passed with the environment variable `JAVA_OPTS`, e.g. `-e JAVA_OPTS="-Xmx2g -Duser.timezone=Europe/Stockholm"`. By default, the maximum heap is 75% of the container's memory limit (`-XX:MaxRAMPercentage=75`); options in `JAVA_OPTS` take precedence.
- RowStore runs as the unprivileged user `rowstore` (UID and GID 10001). The mounted configuration must be readable for it; uploads are buffered in `/tmp` of the container.

To build the image yourself, build the JAR first (the `Dockerfile` copies `target/rowstore-*.jar`):

```
mvn -Dmaven.test.skip=true install
docker build -t rowstore .
```

Note that `localhost` in the configuration refers to the container itself; use a host name or address under which the container can reach PostgreSQL.

## CSV conventions

In order for RowStore to be able to properly process CSV files, some conventions have to be followed:

- String values within the tabular data model (such as column titles or cell string values) must contain only Unicode characters. The character encoding is detected automatically.
- The first row should contain short names for each column; they are used as property names during the CSV2JSON conversion. They are also used as variable names by the query API. Upon import, byte order marks (U+FEFF) and soft hyphens (U+00AD) are removed from the column titles, leading and trailing whitespace and control characters are removed (no-break spaces are kept), and the titles are converted to lower case. Columns with an empty title are skipped.
- A byte order mark at the beginning of the file is skipped before the file is parsed, so it does not become part of the first column title.
- Datasets loaded by RowStore 1.x whose column names contain byte order marks or soft hyphens (e.g. `\ufeff"kopare_id"` instead of `kopare_id`) are renamed in the background after the first start of RowStore 2.0, one dataset at a time, to the names that a fresh import of the same CSV file yields (a byte order mark in front of a quoted first title also left the quotes in the stored name; they are removed as well). A dataset is skipped, logged at WARN level and left unchanged if a cleaned name would be empty or collide with another column name, or if any row contains both an old name and its cleaned name; it keeps its names until it is replaced. An append that arrives before the rename renames the columns itself. Queries keep working during the rename and see the old names until it is complete; only its very end takes the same exclusive lock as a replace (see Data loading), which is brief unless a long export or backup holds the table, in which case the rename waits like a replace. After the rename, such columns can only be queried by their cleaned names.
- Comma ("`,`") must be used as column delimiter. There is some basic detection for CSV-files that are using semi-colon ("`;`") as separator, but it is recommended to use comma.
- Quotation marks ("`"`") must be used as quotation characters; a quotation mark within a quoted value is written as two quotation marks (`""`). With `legacyparser` enabled, backslash ("`\`") is the escape character.
- Line feed ("`\n`") or carriage return followed by line feed ("`\r\n`") must be used to indicate a new line (i.e., a new row).
- Empty lines are only allowed at the end of the file. An empty line between rows, including directly after the header, makes the load fail (the dataset status becomes 4, "Error"). A line consisting of whitespace only is a row, and empty lines within a quoted value are part of the value. Consequently, an empty value in a CSV file with a single column must be quoted (`""`).

## REST API

The API consists of a status resource and resources for handling datasets.

With a few exceptions, all resources respond with JSON, whatever the `Accept` header (no HTTP 406). CSV uploads are sent with `Content-Type: text/csv` or without Content-Type (other types get HTTP 415); alias updates are read as a JSON array whatever their Content-Type.

As in 1.x, clients that cannot set headers or methods can tunnel them: the query parameter `media` replaces the `Accept` header (a file extension such as `json`, `csv`, `html` or a media type, e.g. `/dataset/{id}/export?media=csv`), and the query parameter `method` or the header `X-HTTP-Method-Override` replaces the method of a `POST` request (e.g. `POST /dataset/{id}?method=PUT` replaces the data). Other methods are never tunnelled.

### /datasets

- `GET http://{base-url}/datasets` - Returns an array with all dataset ids.
- `POST http://{base-url}/datasets` - Starts the ETL process for a CSV file. Expects a CSV-file and returns HTTP 202 with location of created dataset. Uploads larger than `maxuploadsize` are rejected with HTTP 413.

### /dataset/{id}

- `GET http://{base-url}/dataset/{id}[?column1=value1&column2=value2&_limit=100&_offset=0]` - Queries the dataset with column/value-tuples, see subsection "Querying" below. Browsers requesting HTML are redirected to the web interface (`/dataset/{id}/html`).
- `PUT http://{base-url}/dataset/{id}` - Replaces existing data, same constraints and parameters apply as for `POST http://{base-url}/datasets`. The previous data is served until the new data has been loaded completely and is kept if the load fails.
- `POST http://{base-url}/dataset/{id}` - Adds data to an existing dataset. The column names must match the existing ones.
- `DELETE http://{base-url}/dataset/{id}` - Deletes the dataset (HTTP 204). Returns HTTP 423 unless the dataset is in status 3 (available) or 4 (error), e.g. while an upload is queued or being loaded.

Uploads to the same dataset are processed one after another in the order in which they were received. A replace (`PUT`) supersedes uploads to the same dataset that are still waiting to be processed; they are discarded.

An alias may be used in the URL instead of the ID above, see below for handling of aliases.

#### Querying

If no tuples are supplied the whole dataset is returned. Tuple values may be regular expressions if the RowStore instance is configured accordingly (see "Configuration" section below).

Results are paginated. Pagination is controlled through the URL parameters `_limit` and `_offset`: `_limit` accepts values from 1 to the configured `querymaxlimit`, otherwise the default of 100 rows is used. The result object contains `limit`, `offset` and `resultCount`, as well as a `results` array with one JSON object per row, and `prev`/`next` links.

The query engine tries to optimize queries by detecting whether a query value contains any characters that are typical for regular expressions. Queries for partial strings may be affected by undesired optimization, in such cases a regexp query can be enforced by prefixing the query value with `~`. E.g. a query for `name=meta` would not trigger a regexp query, whereas a query for `name=^meta` would. To accept `meta` as regexp it must be prefixed with `~`: `name=~meta`.

Queries are subject to an eventually configured query timeout, see configuration section. If a query exceeds the configured timeout the running request to the database is interrupted and HTTP status 503 is returned. Requests for a dataset (`/dataset/{id}` and its sub-resources) also get HTTP 503 if the database cannot be reached, if no database connection becomes available within `connectionWaitTimeout`, or if the dataset is locked (e.g. by a replace waiting for a long export) for longer than `queryLockTimeout` (see [Database connections](#database-connections)).

The parameter `_callback` (JSONP in 1.x) is accepted and ignored: responses are always plain JSON. The parameters `media`, `charset`, `encoding` and `language` are no filters either (see above), unless the dataset has a column of that name.

As in 1.x, parameter names and values are trimmed (`name=Bo%20` matches `Bo`), and regular expression characters such as `^`, `|`, `[`, `{` and `\` may be sent unencoded. Non-ASCII characters must be percent-encoded. A query string with a malformed percent-encoding (e.g. `name=100%`) is rejected with HTTP 400. Regular expressions are not restricted; their run time is limited by `querytimeout`.

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

- `GET http://{base-url}/dataset/{id}/export` - Exports all rows as JSON (`Accept: application/json`, the default) or CSV (`Accept: text/csv` or another text type, or `?media=csv`). If the export cannot start, HTTP 500 is returned (503 if no export connection becomes available in time). A failure after streaming has started aborts the connection, so an incomplete export is recognizable as an incomplete transfer.
- `GET http://{base-url}/dataset/{id}/swagger` - OpenAPI description of the dataset's query interface.
- `GET http://{base-url}/dataset/{id}/html` - Web interface for browsing the dataset.

### /status

- `GET http://{base-url}/status` - Returns some basic information about the RowStore instance: version, number of datasets (-1 if it could not be determined, e.g. because all database connections are in use), the number of loads that are currently running (`activeEtlProcesses`; uploads waiting for their turn are not counted), the number of loads that are queued but have not started yet (`queuedEtlProcesses`), and the usage of the database connection budgets (`connections`, per database and budget: `inUse`, `max`, `waiting`), e.g. `"connections": {"database": {"query": {"inUse": 1, "max": 10, "waiting": 0}, "export": {"inUse": 0, "max": 2, "waiting": 0}, "load": {"inUse": 0, "max": 5, "waiting": 0}}}`.

## Configuration

RowStore is configured through a JSON file, see [Running](#running) for how it is located. `rowstore.example.json` is a minimal example for the [Quickstart](#quickstart), and a complete example follows below.

The configuration is validated at startup. If it is invalid, RowStore logs an error naming the offending key (e.g. `rowstore.json: missing required key 'database.host'`) and does not start. Startup fails if:

- a required key is missing: `baseurl`, `database` with `host`, `database`, `user` and `password` (also in `queryDatabase`, if present); an empty password is allowed,
- `baseurl` or one of the required database keys is not a string,
- `database`, `queryDatabase`, `ratelimit` or `cors` is not a JSON object, or `cors.allowedorigins` is not an array of strings,
- `trustedproxies` is not an array of strings or contains an entry that is not an IP address or CIDR range,
- `querymaxlimit` or `maxuploadsize` is not greater than 0,
- `maxQueryConnections`, `maxExportConnections` or `connectionWaitTimeout` (in `database` or `queryDatabase`) is not a number greater than 0, or `queryLockTimeout` is not a number greater than or equal to 0.

Unknown keys are ignored (e.g. `exportpagesize`, which was never used and has been removed). Optional numeric and boolean values of the wrong type fall back to their defaults.

### Properties

- `baseurl` (String, required) - The base URL under which the root of RowStore can be reached. Used for generating correct URIs in API responses.
- `regexpqueries` (String) - Determines whether the query interface should allow regular expressions to match column values: `disabled` or `false` (no regexp support, default), `simple` (support for queries starting with `^`), and `full` or `true` (support for any regexp queries).
- `maxetlprocesses` (Integer) - Maximum number of concurrently running loads (ETL processes) across all datasets. Values of 0 or less mean the default. Default: 5.
- `legacyparser` (Boolean) - Use OpenCSV's legacy `CSVParser` (backslash as escape character) instead of the RFC 4180 parser. Default: `false`.
- `maxuploadsize` (Integer) - Maximum size of a CSV upload in bytes. Larger uploads are rejected with HTTP 413. Must be greater than 0; there is no setting for unlimited uploads. Default: 104857600 (100 MB).
- `querytimeout` (Integer) - Query timeout in seconds for dataset queries and for the reads of requests that precede them (e.g. dataset and alias lookups, column names, row counts); a timeout results in HTTP 503. By default (or if the value is 0 or less) no query timeout is active, unless configured directly in the database.
- `querymaxlimit` (Integer) - The maximum allowed value of the `_limit` URL parameter when querying a dataset. Must be greater than 0. Default: 100.
- `loglevel` (String) - Log level of RowStore's log output (console): a Log4j level name such as `DEBUG`, `INFO`, `WARN` or `ERROR` (case-insensitive); unknown values mean `INFO`. Default: `INFO`.
- `accesslog` (Boolean) - Log one line per request at INFO level, see [Running](#running). Default: `true`.
- `cors` - Configures CORS.
  - `allowedorigins` (Array of Strings) - Origins that may call RowStore from browsers, e.g. `["https://app.example.com"]`. Default (also if the array is empty): `["*"]` (all origins), see [Security](#security).
- `database` (required) - Configures the database connection. By default, no connection pool is used (connections are opened and closed per operation with `PGSimpleDataSource`). Set `connectionPoolMax` to a positive integer to enable HikariCP connection pooling.
  - `type` - DB type, currently only `postgresql` is supported. Default: `postgresql`.
  - `host` (required) - Hostname.
  - `port` - Port. Default: `5432`.
  - `database` (required) - Name of database.
  - `user` (required) - Username.
  - `password` (required) - Password, may be empty.
  - `ssl` - `true` or `false`. `true` requires SSL (`sslmode=require`), `false` disables it (`sslmode=disable`), so a server that requires SSL refuses the connection. When not set, SSL is used if the server offers it (`sslmode=prefer`, as in 1.7).
  - `connectionPoolMax` - Maximum size of the connection pool. Set to a positive integer to enable HikariCP pooling. When not set or <= 0, no pool is used. Default: -1 (no pool).
  - `connectionPoolInit` - Minimum idle connections in the pool. Only effective when `connectionPoolMax` > 0. Default: -1 (no minimum idle connections).
  - `socketTimeout` - Socket timeout of the database driver in seconds, 0 means unlimited. Default: 0.
  - `connectTimeout` - Connection timeout in seconds, 0 means unlimited. With a connection pool, it also limits the wait for a free pooled connection (with 0, the pool keeps its default of 30 seconds). Default: 5.
  - `loginTimeout` - Login timeout in seconds, 0 means unlimited. Default: 5.
  - `maxQueryConnections` - Maximum number of connections that requests use at the same time for short operations (queries, lookups, alias and dataset changes, health and status checks). Default: 10.
  - `maxExportConnections` - Maximum number of connections for streamed exports at the same time. Only used for the database that serves queries (`queryDatabase` if configured). Default: 2.
  - `connectionWaitTimeout` - Maximum time in seconds a request waits for a free connection before it fails with HTTP 503. Default: `querytimeout` if that is greater than 0, otherwise 30.
  - `queryLockTimeout` - Maximum time in seconds the short operations of requests wait for a lock (PostgreSQL `lock_timeout`, set on each query connection); a lock timeout results in HTTP 503. `0` sets no lock timeout and changes no session state, which is required with PgBouncer transaction pooling (see below). Default: half of `connectionWaitTimeout`.

  See [Database connections](#database-connections) for how these limits work together.
- `queryDatabase` - Configures the database connection for read-only requests, e.g. if queries should be run against a read replica. Same configuration properties as `database`, including the required ones and the connection limits for the queries and exports it serves. Its connections are read-only. Default: the `database` connection.
- `ratelimit` - Configures rate limitation. It is active if `timerange` is greater than 0 and `global` or `dataset` is greater than 0. Only `GET` and `HEAD` requests are rate limited, except for `/status`. Rejected requests get HTTP 429.
  - `type` - `average` or `slidingwindow` (default).
  - `timerange` - The size (in seconds) of the time slot or window to be used for calculating the limitation.
  - `dataset` - Amount of permitted requests per dataset.
  - `global` - Amount of permitted requests globally for a RowStore instance.
  - `clientip` - Amount of permitted requests per client IP. The client IP is taken from the `X-Forwarded-For` header written by the reverse proxy (see `trustedproxies`): by default the last entry of the header, which the proxy has appended or set. Entries further left can be forged by clients and are ignored. Without `X-Forwarded-For`, or if the connecting peer is not a trusted proxy, the client IP is the address of the connecting peer.
- `trustedproxies` (Array of Strings) - IP addresses and CIDR ranges of the reverse proxies whose `X-Forwarded-For` header is trusted, e.g. `["10.0.0.5"]`, `["172.18.0.0/16", "fd00::/8"]`. Only IP addresses are accepted, no host names. Default (if not set): the connecting peer is trusted as the reverse proxy, wherever it is, and the last `X-Forwarded-For` entry is the client IP. This relies on RowStore being reachable only through the reverse proxy (see [Security](#security)); otherwise clients could set the last entry themselves. Set a list to accept `X-Forwarded-For` only from known proxies, or if requests pass through several proxies whose addresses are not resolved before RowStore: the header is then read from right to left, skipping listed addresses, and the first address that is not listed is the client IP. A list should contain only proxy addresses, since clients connecting from a listed range could choose their own client IP, and proxies must write bare addresses (entries with a port, e.g. `192.0.2.1:4711`, count as unlisted). With a list, `X-Forwarded-For` from an unlisted peer is ignored and logged as a warning (at most once per hour per address). An empty array `[]` trusts nobody, i.e. `X-Forwarded-For` is always ignored and all requests through the proxy share the proxy's per-client limit.

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

A CSV upload is loaded asynchronously in a single database transaction; the dataset's status (see `/dataset/{id}/info`) shows the progress. An append (`POST`) inserts the rows into the dataset's table. A replace (`PUT`) loads the rows into a new table and swaps it for the dataset's table at the end of the same transaction (the new table gets the owner and the table privileges of the old one), so queries and exports serve the previous data until the replace is complete, and a failed replace keeps the previous data. During a replace, the database needs disk space for both the old and the new data. The swap has to wait for running readers of the table, e.g. a long export or a backup with `pg_dump`. It therefore tries to lock the table for at most 3 seconds at a time and retries with growing delays (up to 30 seconds), so that requests for the dataset are delayed by at most about 3 seconds at a time and still succeed, and requests for other datasets are not affected. The swap is retried for at most 30 minutes; if the table is still in use then (e.g. by a backup or a database session left idle in an open transaction), the replace fails with status 4 and the previous data stays. The sessions holding the table are logged, so that an operator can see what blocked it.

Since a replace drops the old table, only its owner and table privileges are carried over to the new one. Anything else that was added to a data table outside of RowStore is lost with the first replace (manually created indexes, triggers, comments, storage parameters such as per-table autovacuum settings, column privileges, membership in publications that list the table), and views on a data table make every replace of the dataset fail (status 4, keeping the previous data). In 1.x, a replace emptied the table with `TRUNCATE` and kept such objects.

A dataset that is uploaded as several files (a `PUT` followed by `POST`s) is replaced atomically per file only: after the `PUT`, clients see the first file's rows until the last `POST` has been loaded. Check the status after each upload: if one of the files fails, the next successful upload of the dataset sets the status back to 3 ("Available"). An atomic replace with several files is planned ([ROWS-150](https://metasolutions.atlassian.net/browse/ROWS-150)).

## Database connections

By default, RowStore does not pool connections: it opens a connection per operation and closes it afterwards, so an idle instance holds no connections. The number of connections that an instance opens at the same time is limited per database by three budgets:

- **query** (`maxQueryConnections`, default 10): short operations of requests,
- **export** (`maxExportConnections`, default 2): streamed exports, which hold their connection while the export is transferred,
- **load** (`maxetlprocesses`, default 5): data loads and all other database work of loads, including their status updates, so loads always finish even if all query connections are busy.

A request that finds all connections of its budget in use waits for a free one (first come, first served) for at most `connectionWaitTimeout` and then gets HTTP 503. The defaults are meant to absorb bursts by waiting. Requests that are blocked by a lock on a dataset for a long time (e.g. by a manual `LOCK TABLE`) give up their connection after `queryLockTimeout` with HTTP 503, so that requests for other datasets keep being served. Responses with HTTP 503 because of a busy or unavailable database carry `Retry-After: 5`. The current usage is reported by `/status` and, if health details are enabled (`management.endpoint.health.show-details`), in the details of `/actuator/health`; waits longer than one second and timeouts are logged at WARN level, aggregated to at most one message per minute.

### Sizing many instances

In the worst case, one instance opens per database `maxQueryConnections + maxExportConnections + maxetlprocesses` connections, i.e. 10 + 2 + 5 = 17 with the defaults. With a separate `queryDatabase`, the primary database gets `database.maxQueryConnections + maxetlprocesses` and the query database `queryDatabase.maxQueryConnections + queryDatabase.maxExportConnections`. The sum over all instances that use the same PostgreSQL cluster (each with its own database, see [Running](#running)) must stay below its `max_connections` minus the connections reserved for administration and other clients: e.g. 80 instances with the defaults may open 80 x 17 = 1360 connections. For such deployments, lower the limits per instance or put PgBouncer in transaction pooling mode in front of the cluster (see below).

With `connectionPoolMax` > 0, connections are pooled with HikariCP and idle connections are kept; the budgets apply on top of the pool, which should be at least as large as the sum of the budgets of its database (a warning is logged at startup otherwise). Since 1.x never pooled the connections of `database` (and those of `queryDatabase` only if `connectionPoolInit` was greater than 0 as well), a warning is logged at startup when 2.0 pools connections that 1.x did not, e.g. because a `connectionPoolMax` from a 1.x configuration is now effective.

If the database is not reachable at startup (e.g. during a restart or failover of the cluster), RowStore waits for it: the schema migration and the startup maintenance are retried with a delay of 1 second, doubled up to 60 seconds, without limit, and each attempt is logged at WARN level. Meanwhile RowStore does not listen on its port. Other errors, e.g. wrong credentials or a failing migration, still end the startup.

## Connection Pooling with PgBouncer

When running many RowStore instances against a shared PostgreSQL cluster, a connection pooler such as [PgBouncer](https://www.pgbouncer.org/) is recommended to avoid connection exhaustion.

### When to use PgBouncer

By default, RowStore uses `PGSimpleDataSource` (no connection pool), which opens and closes connections per operation. This is appropriate for most deployments. However, when dozens or hundreds of instances share a single PostgreSQL cluster, even short-lived connections can cause contention on the server side.

PgBouncer in `transaction` mode multiplexes many application connections over a smaller number of PostgreSQL server connections, reducing load on the database. RowStore only uses session state within transactions, except for the lock timeout of query connections (`queryLockTimeout`), which is set with `SET lock_timeout` when a connection is obtained. Under transaction pooling, such a setting would end up on an arbitrary server connection and stay there for other clients, and PgBouncer cannot track it (`track_extra_parameters` only covers parameters that PostgreSQL reports to the client, and `lock_timeout` is not one of them). Connections of `queryDatabase` also set the session read-only (`SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY`, driver `readOnlyMode=always`), which PgBouncer cannot track either, so behind PgBouncer `queryDatabase` must use its own database user (its own PgBouncer pool), never the user of `database`. Therefore, with transaction pooling:

- set `queryLockTimeout` to `0` in `database` and `queryDatabase`, and
- configure the lock timeout on the server side instead, preferably for a separate database user of the query database, e.g. `ALTER ROLE rowstore_query SET lock_timeout = '15s'` (half of `connectionWaitTimeout`). If the same user also runs loads and exports, the setting applies to them as well: replaces set their own lock timeouts, but an export blocked behind an exclusive lock for longer fails.

### Recommended PgBouncer configuration

```ini
[databases]
rowstore = host=postgres-main port=5432 dbname=rowstore

[pgbouncer]
pool_mode = transaction
default_pool_size = 20
max_client_conn = 1000
max_prepared_statements = 200
```

The PostgreSQL JDBC driver switches to server-side prepared statements after a statement has been executed five times on a connection (`prepareThreshold=5`), and RowStore does not change that setting. In `transaction` mode, this requires PgBouncer 1.21 or later with `max_prepared_statements` greater than 0, otherwise requests fail with errors about missing or already existing prepared statements.

### RowStore configuration with PgBouncer

Point RowStore at PgBouncer instead of PostgreSQL directly. Connection pooling in RowStore itself is typically not needed when using PgBouncer:

```json
{
  "database": {
    "host": "pgbouncer-host",
    "port": 6432,
    "database": "rowstore",
    "user": "rowstore",
    "password": "secret",
    "queryLockTimeout": 0
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
    "queryLockTimeout": 0,
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
- set `X-Forwarded-For` (append the client address or replace the header with it) and, if `trustedproxies` is configured, be listed there (see [Configuration](#properties)), so that the per-client rate limit applies to the real client and cannot be influenced by clients.

A `POST` request can be tunnelled to `PUT` or `DELETE` with the query parameter `method` or the header `X-HTTP-Method-Override` (see [REST API](#rest-api)). Since the proxy authenticates `POST` like `PUT` and `DELETE`, this does not open the private API. A proxy that authorizes methods differently (e.g. allows `POST` to more users than `DELETE`) must reject or strip the `method` parameter and the `X-HTTP-Method-Override` header of `POST` requests. `GET` and `HEAD` requests are never tunnelled.

CORS: by default all origins are allowed (`*`). This is acceptable only under the contract above: the read API is public anyway, and RowStore never sends `Access-Control-Allow-Credentials`, so web pages of other origins cannot use a browser's proxy credentials to read responses of the private API. To allow only known web applications to call RowStore from browsers, list their origins in `rowstore.json`, e.g. `"cors": { "allowedorigins": ["https://app.example.com"] }`. CORS is handled before the rate limit, so rate-limited responses (429) carry CORS headers, and preflight requests (`OPTIONS`) are answered without counting towards the rate limit.

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

    # Replace any client-supplied value; if "trustedproxies" is configured, it must list this proxy
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

The example allows only the public API paths and denies everything else. If a load balancer or CDN sits in front of nginx, configure nginx's real IP module (`set_real_ip_from <balancer addresses>; real_ip_header X-Forwarded-For; real_ip_recursive on;`) so that `$remote_addr` is the real client address, and keep `proxy_set_header X-Forwarded-For $remote_addr` instead of `$proxy_add_x_forwarded_for`. Note that `limit_except GET HEAD` also requires authentication for `OPTIONS`, so CORS preflight requests to `/dataset*` (needed only for the private API) get 401; this is acceptable under the contract above.

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

A table records one-time maintenance tasks that have completed, so that later starts skip them (currently the column name cleanup, row `column-name-cleanup`; deleting the row makes it run again):

`CREATE TABLE IF NOT EXISTS maintenance_tasks (name TEXT PRIMARY KEY, completed TIMESTAMP NOT NULL)`

## License

[MetaSolutions AB](http://www.metasolutions.se) licenses this work under the terms of the Apache License 2.0 (the "License"); you may not use this work except in compliance with the License. See the `LICENSE.txt` file distributed with this work for the full License.
