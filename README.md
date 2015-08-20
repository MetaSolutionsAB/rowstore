# RowStore [![Build Status](https://drone.io/bitbucket.org/metasolutions/rowstore/status.png)](https://drone.io/bitbucket.org/metasolutions/rowstore/latest)

RowStore is a minimum footprint storage and query engine for tabular data with two main purposes:

1. A pipeline to transform of tabular data (CSV) to JSON and to load the transformed data into a JSON-optimized store (PostgreSQL), and
2. A query interface to fetch all rows that match certain column values.

## Security

RowStore provides a private REST API (for data management) and a well as a public REST API (for data retrieval). Currently RowStore does not provide any own security mechanism and it is recommended that the private API is protected by a web server/reverse proxy with authentication features such as Apache HTTPD.

## Issue tracking

Please refer to [tbd](https://tbd) for issue tracking.

## License

[MetaSolutions AB](http://www.metasolutions.se) licenses this work under the terms of the Apache License 2.0 (the "License"); you may not use this file except in compliance with the License. See the `LICENSE.txt` file distributed with this work for the full License.
