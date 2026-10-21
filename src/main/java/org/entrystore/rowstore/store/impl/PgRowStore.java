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

package org.entrystore.rowstore.store.impl;

import jakarta.annotation.PreDestroy;
import org.entrystore.rowstore.etl.EtlProcessor;
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.resources.VersionInfo;
import org.entrystore.rowstore.store.Datasets;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

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
		resetInterruptedLoads();
		log.info("Started RowStore {}", versionInfo.getVersion());
	}

	/**
	 * Sets the status of all datasets in PROCESSING to ERROR. Called at startup: since a database must not be used
	 * by more than one RowStore instance, such datasets were left behind by loads that were interrupted, e.g., by a
	 * crash. Datasets in ACCEPTED_DATA are left untouched.
	 *
	 * @return Returns the IDs of the datasets that were reset.
	 */
	public List<String> resetInterruptedLoads() {
		try {
			List<String> ids = jdbcTemplate.queryForList(
					"UPDATE " + PgDatasets.DATASETS_TABLE_NAME + " SET status = ? WHERE status = ? RETURNING id::text",
					String.class, EtlStatus.ERROR, EtlStatus.PROCESSING);
			if (!ids.isEmpty()) {
				log.warn("Set status of {} dataset(s) from {} to {} because their loads were interrupted: {}",
						ids.size(), EtlStatus.toString(EtlStatus.PROCESSING), EtlStatus.toString(EtlStatus.ERROR), ids);
			}
			return ids;
		} catch (DataAccessException e) {
			log.error("Unable to reset datasets with interrupted loads: {}", e.getMessage());
			return List.of();
		}
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
