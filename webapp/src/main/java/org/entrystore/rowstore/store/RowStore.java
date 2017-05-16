/*
 * Copyright (c) 2011-2015 MetaSolutions AB <info@metasolutions.se>
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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Hannes Ebner
 */
public interface RowStore {

	/**
	 * @return Returns a database connection.
	 * @throws SQLException
	 */
	Connection getConnection() throws SQLException;

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