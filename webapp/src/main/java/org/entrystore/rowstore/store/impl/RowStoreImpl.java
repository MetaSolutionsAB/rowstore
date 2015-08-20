package org.entrystore.rowstore.store.impl;

import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.postgresql.ds.PGPoolingDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Hannes Ebner
 */
public class RowStoreImpl implements RowStore {

	DataSource ds;

	public RowStoreImpl(RowStoreConfig config) {
		if (config == null) {
			throw new IllegalArgumentException("Configuration must not be null");
		}
		ds = new PGPoolingDataSource();
		PGPoolingDataSource pgDs = (PGPoolingDataSource) ds;
		pgDs.setMaxConnections(config.getDbMaxConnections());
		pgDs.setUser(config.getDbUser());
		pgDs.setPassword(config.getDbPassword());
		pgDs.setServerName(config.getDbHost());
		pgDs.setDatabaseName(config.getDbName());
	}

	@Override
	public Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

}