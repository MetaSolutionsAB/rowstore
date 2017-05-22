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

package org.entrystore.rowstore.store.impl;

import org.entrystore.rowstore.etl.EtlProcessor;
import org.entrystore.rowstore.store.Datasets;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * A PostgreSQL-specific implementation of the RowStore interface.
 *
 * @author Hannes Ebner
 * @see RowStore
 */
public class PgRowStore implements RowStore {

	private static Logger log = LoggerFactory.getLogger(PgRowStore.class);

	DataSource datasource;

	Datasets datasets;

	EtlProcessor etlProcessor;

	RowStoreConfig config;

	public PgRowStore(RowStoreConfig config) {
		if (config == null) {
			throw new IllegalArgumentException("Configuration must not be null");
		}
		this.config = config;

		//datasource = new PGPoolingDataSource();
		datasource = new PGSimpleDataSource();
		PGSimpleDataSource pgDs = (PGSimpleDataSource) datasource;
		//pgDs.setMaxConnections(config.getDbMaxConnections());
		pgDs.setUser(config.getDbUser());
		pgDs.setPassword(config.getDbPassword());
		pgDs.setServerName(config.getDbHost());
		pgDs.setDatabaseName(config.getDbName());
		pgDs.setPortNumber(config.getDbPort());
		pgDs.setSsl(config.getDbSsl());
		if (!config.getDbSsl()) {
			try {
				pgDs.setProperty("sslMode", "disable");
			} catch (SQLException e) {
				SqlExceptionLogUtil.error(log, e);
			}
		}

		etlProcessor = new EtlProcessor(this);
	}

	/**
	 * @see RowStore#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException {
		return datasource.getConnection();
	}

	/**
	 * @see RowStore#getEtlProcessor()
	 */
	@Override
	public EtlProcessor getEtlProcessor() {
		return etlProcessor;
	}

	public Datasets getDatasets() {
		synchronized (datasource) {
			if (datasets == null) {
				this.datasets = new PgDatasets(this);
			}
		}
		return this.datasets;
	}

	/**
	 * @see RowStore#getConfig()
	 */
	@Override
	public RowStoreConfig getConfig() {
		return config;
	}

	/**
	 * @see RowStore#shutdown()
	 */
	@Override
	public void shutdown() {
		log.info("Shutting down RowStore");
		etlProcessor.shutdown();
	}

}