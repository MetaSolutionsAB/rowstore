package org.entrystore.rowstore.store.impl;

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

	public PgRowStore(RowStoreConfig config) {
		if (config == null) {
			throw new IllegalArgumentException("Configuration must not be null");
		}
		datasource = new PGPoolingDataSource();
		PGPoolingDataSource pgDs = (PGPoolingDataSource) datasource;
		pgDs.setMaxConnections(config.getDbMaxConnections());
		pgDs.setUser(config.getDbUser());
		pgDs.setPassword(config.getDbPassword());
		pgDs.setServerName(config.getDbHost());
		pgDs.setDatabaseName(config.getDbName());
	}

	@Override
	public Connection getConnection() throws SQLException {
		return datasource.getConnection();
	}

	public Datasets getDatasets() {
		synchronized (datasource) {
			if (datasets == null) {
				this.datasets = new PgDatasets(this);
			}
		}
		return this.datasets;
	}

}