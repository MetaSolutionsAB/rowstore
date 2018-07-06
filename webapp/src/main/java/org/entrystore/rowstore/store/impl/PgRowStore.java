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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

/**
 * A PostgreSQL-specific implementation of the RowStore interface.
 *
 * @author Hannes Ebner
 * @see RowStore
 */
public class PgRowStore implements RowStore {

	private static Logger log = LoggerFactory.getLogger(PgRowStore.class);

	DataSource datasource;

	DataSource queryDatasource;

	Datasets datasets;

	EtlProcessor etlProcessor;

	RowStoreConfig config;

	public PgRowStore(RowStoreConfig config) {
		if (config == null) {
			throw new IllegalArgumentException("Configuration must not be null");
		}
		this.config = config;

		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			log.error(e.getMessage());
		}

		//datasource = new PGPoolingDataSource();
		datasource = initializeDataSource(new PGSimpleDataSource(), config.getDatabase());
		if (config.getDatabase() == config.getQueryDatabase()) {
			queryDatasource = datasource;
		} else {
			queryDatasource = initializeDataSource(new PGSimpleDataSource(), config.getQueryDatabase());
		}

		etlProcessor = new EtlProcessor(this);
	}

	private PGSimpleDataSource initializeDataSource(PGSimpleDataSource dataSource, RowStoreConfig.Database dbConfig) {
		if (dataSource == null || dbConfig == null) {
			throw new IllegalArgumentException("Parameters must not be null");
		}

		//dataSource.setMaxConnections(dbConfig.getMaxConnections());
		dataSource.setUser(dbConfig.getUser());
		dataSource.setPassword(dbConfig.getPassword());
		dataSource.setServerName(dbConfig.getHost());
		dataSource.setDatabaseName(dbConfig.getName());
		dataSource.setPortNumber(dbConfig.getPort());
		dataSource.setSsl(dbConfig.getSsl());
		if (dataSource.getSsl()) {
			dataSource.setSslMode("require");
		}
		return dataSource;
	}

	/**
	 * @see RowStore#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException {
		return datasource.getConnection();
	}

	/**
	 * @see RowStore#getQueryConnection()
	 */
	@Override
	public Connection getQueryConnection() throws SQLException {
		return queryDatasource.getConnection();
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

		// Deregister JDBC driver that were loaded by this webapp
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		Enumeration<Driver> drivers = DriverManager.getDrivers();
		while (drivers.hasMoreElements()) {
			Driver driver = drivers.nextElement();
			if (driver.getClass().getClassLoader() == cl) {
				// This driver was registered by the webapp's ClassLoader, so deregister it
				try {
					log.info("Deregistering JDBC driver: {}", driver);
					DriverManager.deregisterDriver(driver);
				} catch (SQLException ex) {
					log.error("An error occured when deregistering JDBC driver: {}", driver, ex);
				}
			} else {
				log.trace("Not deregistering JDBC driver {} as it does not belong to this webapp's ClassLoader", driver);
			}
		}
	}

}