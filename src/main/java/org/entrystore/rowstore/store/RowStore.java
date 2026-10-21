/*
 * Copyright (c) 2011-2026 MetaSolutions AB <info@metasolutions.se>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.entrystore.rowstore.store;

import org.entrystore.rowstore.etl.EtlProcessor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * The connections and JdbcTemplates are limited by connection budgets (ARCH-7). A connection must be closed after use;
 * while it is open, no further connection must be obtained (from any budget), otherwise concurrent requests may wait
 * for each other until the wait timeout. If no connection can be obtained, e.g., because no connection of the budget
 * became free within the wait timeout, the connection methods throw a CannotGetJdbcConnectionException (a
 * DataAccessResourceFailureException, like JdbcTemplate does), which results in 503 (API-6.05).
 *
 * @author Hannes Ebner
 */
public interface RowStore {

	/**
	 * @return Returns a database connection for short write and read operations (query budget of the primary database).
	 * @throws SQLException
	 */
	Connection getConnection() throws SQLException;

	/**
	 * @return Returns a database connection for short read operations (query budget, may be a read replica).
	 * @throws SQLException
	 */
	Connection getQueryConnection() throws SQLException;

	/**
	 * @return Returns a database connection for an export (export budget, may be a read replica).
	 * @throws SQLException
	 */
	default Connection getExportConnection() throws SQLException {
		return getQueryConnection();
	}

	/**
	 * @return Returns a database connection for loading data into a dataset or renaming its columns (load budget of
	 * the primary database). Must only be used by ETL jobs, which are limited by maxetlprocesses.
	 * @throws SQLException
	 */
	default Connection getLoadConnection() throws SQLException {
		return getConnection();
	}

	/**
	 * @return Returns a JdbcTemplate for short write and read operations (query budget of the primary database).
	 */
	JdbcTemplate getJdbcTemplate();

	/**
	 * @return Returns a JdbcTemplate for short read operations (query budget, may target a read replica).
	 */
	JdbcTemplate getQueryJdbcTemplate();

	/**
	 * @return Returns the usage of the connection budgets per database block ("database", "queryDatabase") and
	 * budget ("query", "export", "load").
	 */
	default Map<String, Map<String, ConnectionUsage>> getConnectionUsage() {
		return Map.of();
	}

	/**
	 * @return Returns an instance of the dataset manager.
	 */
	Datasets getDatasets();

	/**
	 * @return Returns the global configuration.
	 */
	RowStoreConfig getConfig();

	/**
	 * @return Returns the ETL processor.
	 */
	EtlProcessor getEtlProcessor();

	/**
	 * Initiates a graceful shutdown.
	 */
	void shutdown();

}