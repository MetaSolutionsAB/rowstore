# RowStore

RowStore is a minimum footprint storage and query engine for tabular data with two main purposes:

1. A pipeline to transform of tabular data (CSV) to JSON and to load the transformed data into a JSON-optimized store (PostgreSQL), and
2. A query interface to fetch all rows that match certain column values.

## CSV conventions

In order for RowStore to be able to properly process CSV files, some conventions have to be followed:

- String values within the tabular data model (such as column titles or cell string values) must contain only Unicode characters.
- The first row should contain short names for each column; they are used as property names during the CSV2JSON conversion. They are also used as variable names by the query API. The column titles are converted to lower case upon import.
- Comma ("`,`") must be used as column delimiter. There is some basic detection for CSV-files that are using semi-colon ("`;`") as separator, but it is recommended to use comma.
- Quotation marks ("`"`") must be used as quotation characters.
- Double backslash ("`\\`) must be used as escape characters.
- Line feed ("`\n`") or carriage return followed by line feed ("`\r\n`") must be used to indicate a new line (i.e., a new row).

## REST API

The API consists of three resources; one to request the service's status and two for handling datasets.

With a few exceptions, all resources expect JSON payloads.

### /datasets

- `GET http://{base-url}/datasets` - Returns an array with all dataset ids. 
- `POST http://{base-url}/datasets` - Starts the ETL process for a CSV file. Expects a CSV-file and returns HTTP 202 with location of created dataset.

### /dataset/{id}

- `GET http://{base-url}/dataset/{id}[?column1=value1&column2=value2&_limit=100&_offset=0]` - Queries the dataset with column/value-tuples, se subsection "Querying" below.
- `PUT http://{base-url}/dataset/{id}` - Replaces existing data, same contraints and parameters apply as for `POST http://{base-url}/datasets`.
- `POST http://{base-url}/dataset/{id}` - Adds data to existing dataset. No structural integrity check is carried out, so it is possible to add data with a different field structure (i.e. column names). It is up to the client to enforce a consistent structure, if needed.
- `DELETE http://{base-url}/dataset/{id}` - Deletes the dataset.

An alias may be used in the URL instead of the ID above, see below for handling of aliases.

#### Querying

If no tuples are supplied the whole dataset is returned. Tuple values may be regular expressions if the RowStore instance is configured accordingly (see "Configuration" section below).

Pagination is supported and enforced for result sets larger than 100 rows. Pagination is controlled through the URL parameters `_limit` (expects a value from 1 to 100) and `_offset`. The result object contains `limit`, `offset` and `resultCount`, as well as a `results` array with one JSON object per row.

The query engine tries to optimize queries by detecting whether a query value contains any characters that are typical for regular expressions. Queries for partial strings may be affected by undesired optimization, in such cases a regexp query can be enforced by prefixing the query value with `~`. E.g. a query for `name=meta` would not trigger a regexp query, whereas a query for `name=^meta` would. To accept `meta` as regexp it must be prefixed with `~`: `name=~meta`.

Queries are subject to an eventually configured query timeout, see configuration section. If a query exceeds the configured timeout the running request to the database is interrupted and a response body is returned containing an explanatory message and HTTP status 503.

### /dataset/{id}/info

- `GET http://{base-url}/dataset/{id}/info` - Returns information (e.g. status) about a dataset.

Example information object:

```
{
  "status": 3,
  "created": 2015-04-23T12:20:43.511Z,
  "columnnames": ["Station", "Lat", "Long", "Air quality"],
  "rowcount": 342,
  "aliases": ["alias1", "alias2"]
}
```

Available status values:

- 0: Created
- 1: Accepted data
- 2: Processing
- 3: Available
- 4: Error

### /dataset/{id}/aliases

- `GET http://{base-url}/dataset/{id}/aliases` - Returns a JSON array with all aliases of a dataset. This information is also included in the info-object (see above for information about the info-resource).
- `PUT http://{base-url}/dataset/{id}/aliases` - Sets (and replaces if applicable) aliases of the dataset, expects a JSON array.
- `POST http://{base-url}/dataset/{id}/aliases` - Adds an alias, does not replace existing datasets.
- `DELETE http://{base-url}/dataset/{id}/aliases` - Removes all aliases of a dataset.

Aliases may only contain alpha-numeric characters (including Unicode) and may be used as a replacement for the dataset's ID when requesting data. Aliases are basically "nice names" for a dataset.

### /status

- `GET http://{base-url}/status` - Returns some basic information about the RowStore instance. 

## Configuration

RowStore is configured through a simple JSON-file. The distribution contains an example file.

### Properties

- `baseurl` (String) - The base URL under which the root of RowStore can be reached. Used for generating correct URIs in API responses.
- `regexpqueries` (String) - Determines whether the query interface should allow regular expressions to match column values. Differentiates between `disabled` (no regexp support), `simple` (support for queries starting with `^`), and `full` (support for any regexp queries).
- `maxetlprocesses` (Integer) - Maximum number of concurrently running ETL processes (each process takes up one thread).
- `database` (parent object) - Configures the database connection.
- `loglevel` (String) - Determines the log level. Possible values: `DEBUG`, `INFO`, `WARN`, `ERROR`. Only relevant if run standalone; if run in a container (e.g. Tomcat) please refer to the container's logging configuration.
- `querytimeout` (Integer) - Configures query timeout for dataset-queries in seconds. By default no query timeout is active (unless configured directly in the database).
- `ratelimit` - Configures rate limitation.
    - `type` - `average` or `slidingwindow` (default).
    - `timerange` - The size (in seconds) of the time slot or window to be used for calculating the limitation.
    - `dataset` - Amount of permitted requests per dataset.
    - `global` - Amount of permitted requests globally for a RowStore instance.

### Example

```
{
  "baseurl": "https://domain.com/rowstore/",
  "regexpqueries": "full",
  "maxetlprocesses": 5,
  "database": {
    "type": "postgresql",
    "host": "localhost",
    "database": "rowstore",
    "user": "rowstore",
    "password": ""
  },
  "ratelimit": {
    "type": "slidingwindow",
    "timerange": 60,
    "dataset": 30,
    "global": 300
  },
  "loglevel": "debug"
}
```

## Installation

RowStore is built using Maven:

```
mvn -Dmaven.test.skip=true install
```

Successfully built, the WAR-file can be deployed as webapp in e.g. Tomcat, or be run standalone:

```
chmod +x standalone/target/dist/bin/rowstore
standalone/target/dist/bin/rowstore <path-to-configuration.json> [port-number]
```

The port number is optional, by default port 8282 is used.

## Security

RowStore provides a private REST API (for data management) as well as a public REST API (for data retrieval). Currently RowStore does not provide any own security mechanism and it is recommended that the private API is protected by a web server/reverse proxy with authentication features such as Apache HTTPD.

## Issue tracking

Please refer to [RowStore on GitHub](https://github.com/MetaSolutionsAB/rowstore/issues) for issue tracking.

## Database layout

An administrative table keeps track of datasets and their current status:

`CREATE TABLE IF NOT EXISTS datasets (id UUID PRIMARY KEY, status INT NOT NULL, created TIMESTAMP NOT NULL, data_table CHAR(37))`

A table per dataset holds the actual data in JSON:

`CREATE TABLE IF NOT EXISTS {data-table} (rownr SERIAL, data JSONB NOT NULL)`

A table to manage aliases:

`CREATE TABLE IF NOT EXISTS aliases (id SERIAL, dataset_id UUID NOT NULL, alias TEXT NOT NULL)`

## Roadmap

### Version 1.0

- Support for [_CSV2JSON minimal mode_](http://www.w3.org/TR/csv2json/#dfn-minimal-mode)
- Support for PostgreSQL as backend

### Version 1.1

- Support for [_CSV2JSON standard mode_](http://www.w3.org/TR/csv2json/#dfn-standard-mode)
- Support for MySQL as backend

### Version 1.2

- Support for [_Tabular data model_](http://www.w3.org/TR/tabular-data-model/)

### Version 2.0

- Support for [_CSV2RDF_](http://www.w3.org/TR/csv2rdf/)

## License

[MetaSolutions AB](http://www.metasolutions.se) licenses this work under the terms of the Apache License 2.0 (the "License"); you may not use this work except in compliance with the License. See the `LICENSE.txt` file distributed with this work for the full License.
