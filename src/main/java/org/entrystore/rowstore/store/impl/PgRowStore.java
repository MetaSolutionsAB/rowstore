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

import jakarta.annotation.PreDestroy;
import org.entrystore.rowstore.etl.EtlProcessor;
import org.entrystore.rowstore.resources.VersionInfo;
import org.entrystore.rowstore.store.Datasets;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * A PostgreSQL-specific implementation of the RowStore interface.
 *
 * @author Hannes Ebner
 * @see RowStore
 */
@Service
public class PgRowStore implements RowStore {

	private final static Logger log = LoggerFactory.getLogger(PgRowStore.class);

	private final DataSource datasource;

	private final DataSource queryDatasource;

	private final JdbcTemplate jdbcTemplate;

	private final JdbcTemplate queryJdbcTemplate;

	private Datasets datasets;

	private final EtlProcessor etlProcessor;

	private final RowStoreConfig config;

	public PgRowStore(DataSource datasource,
					  @Qualifier("queryDataSource") DataSource queryDatasource,
					  RowStoreConfig config,
					  VersionInfo versionInfo) {
		if (config == null) {
			throw new IllegalArgumentException("Configuration must not be null");
		}
		this.config = config;
		this.datasource = datasource;
		this.queryDatasource = queryDatasource;
		this.jdbcTemplate = new JdbcTemplate(datasource);
		this.queryJdbcTemplate = new JdbcTemplate(queryDatasource);

		etlProcessor = new EtlProcessor(this);
		log.info("Started RowStore {}", versionInfo.getVersion());
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

	@Override
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	@Override
	public JdbcTemplate getQueryJdbcTemplate() {
		return queryJdbcTemplate;
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
	@PreDestroy
	public void shutdown() {
		log.info("Shutting down RowStore");
		etlProcessor.shutdown();
		log.info("Shutdown complete");
	}

}
