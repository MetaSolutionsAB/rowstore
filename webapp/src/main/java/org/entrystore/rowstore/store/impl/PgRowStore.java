package org.entrystore.rowstore.store.impl;

import org.entrystore.rowstore.etl.EtlProcessor;
import org.entrystore.rowstore.store.Datasets;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.postgresql.ds.PGPoolingDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Hannes Ebner
 */
public class PgRowStore implements RowStore {

	DataSource datasource;

	Datasets datasets;

	EtlProcessor etlProcessor;

	RowStoreConfig config;

	public PgRowStore(RowStoreConfig config) {
		if (config == null) {
			throw new IllegalArgumentException("Configuration must not be null");
		}
		this.config = config;

		datasource = new PGPoolingDataSource();
		PGPoolingDataSource pgDs = (PGPoolingDataSource) datasource;
		pgDs.setMaxConnections(config.getDbMaxConnections());
		pgDs.setUser(config.getDbUser());
		pgDs.setPassword(config.getDbPassword());
		pgDs.setServerName(config.getDbHost());
		pgDs.setDatabaseName(config.getDbName());

		etlProcessor = new EtlProcessor(this);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return datasource.getConnection();
	}

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

	@Override
	public RowStoreConfig getConfig() {
		return config;
	}

	@Override
	public void shutdown() {
		etlProcessor.shutdown();
	}

}