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
import org.postgresql.ds.PGPoolingDataSource;
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

	private final static Logger log = LoggerFactory.getLogger(PgRowStore.class);

	final DataSource datasource;

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

		if (config.getDatabase().getConnectionPoolInit() > 0 && config.getDatabase().getConnectionPoolMax() > 0) {
			datasource = initializeDataSource(new PGPoolingDataSource(), config.getDatabase());
		} else {
			datasource = initializeDataSource(new PGSimpleDataSource(), config.getDatabase());
		}

		if (config.getDatabase() == config.getQueryDatabase()) {
			queryDatasource = datasource;
		} else {
			if (config.getQueryDatabase().getConnectionPoolInit() > 0 && config.getQueryDatabase().getConnectionPoolMax() > 0) {
				queryDatasource = initializeDataSource(new PGPoolingDataSource(), config.getQueryDatabase());
			} else {
				queryDatasource = initializeDataSource(new PGSimpleDataSource(), config.getQueryDatabase());
			}
		}

		etlProcessor = new EtlProcessor(this);
	}

	private DataSource initializeDataSource(DataSource dataSource, RowStoreConfig.Database dbConfig) {
		if (dataSource == null || dbConfig == null) {
			throw new IllegalArgumentException("Parameters must not be null");
		}

		if (dataSource instanceof  PGSimpleDataSource) {
			PGSimpleDataSource ds = (PGSimpleDataSource) dataSource;
			ds.setUser(dbConfig.getUser());
			ds.setPassword(dbConfig.getPassword());
			ds.setServerName(dbConfig.getHost());
			ds.setDatabaseName(dbConfig.getName());
			ds.setPortNumber(dbConfig.getPort());
			ds.setSsl(dbConfig.getSsl());
			if (ds.getSsl()) {
				ds.setSslMode("require");
			}
		} else if (dataSource instanceof  PGPoolingDataSource) {
			PGPoolingDataSource ds = (PGPoolingDataSource) dataSource;
			ds.setInitialConnections(dbConfig.getConnectionPoolInit());
			ds.setMaxConnections(dbConfig.getConnectionPoolMax());
			ds.setUser(dbConfig.getUser());
			ds.setPassword(dbConfig.getPassword());
			ds.setServerName(dbConfig.getHost());
			ds.setDatabaseName(dbConfig.getName());
			ds.setPortNumber(dbConfig.getPort());
			ds.setSsl(dbConfig.getSsl());
			if (ds.getSsl()) {
				ds.setSslMode("require");
			}
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
		log.info("Shutdown complete");
	}

}