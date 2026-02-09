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

import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.Datasets;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * A PostgreSQL-specific implementation of the Datasets interface.
 *
 * @author Hannes Ebner
 * @see Datasets
 */
public class PgDatasets implements Datasets {

	private static Logger log = LoggerFactory.getLogger(PgDatasets.class);

	protected static String DATASETS_TABLE_NAME = "datasets";

	protected static String ALIAS_TABLE_NAME = "aliases";

	PgRowStore rowstore;

	protected PgDatasets(PgRowStore rowstore) {
		this.rowstore = rowstore;
		// Schema creation is handled by Flyway
	}

	/**
	 * @see Datasets#getAll()
	 */
	@Override
	public Set<Dataset> getAll() {
		long before = System.currentTimeMillis();
		Set<Dataset> result = null;
		try (Connection conn = getRowStore().getConnection();
			 PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + DATASETS_TABLE_NAME);
			 ResultSet rs = stmt.executeQuery()) {
			log.debug("Executing: " + stmt);
			result = new HashSet<>();
			while (rs.next()) {
				UUID id = (UUID) rs.getObject("id");
				int status = rs.getInt("status");
				Timestamp created = rs.getTimestamp("created");
				String dataTable = rs.getString("data_table");
				result.add(new PgDataset(rowstore, id.toString(), status, created, dataTable));
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			log.debug("Fetching datasets took {} ms", System.currentTimeMillis() - before);
		}

		return result;
	}

	/**
	 * @see Datasets#createDataset()
	 */
	@Override
	public Dataset createDataset() {
		long before = System.currentTimeMillis();
		String id = createUniqueDatasetId();
		String dataTable = constructDataTableName(id);
		try (Connection conn = getRowStore().getConnection()) {
			conn.setAutoCommit(false);

			try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + DATASETS_TABLE_NAME + " (id, status, created, data_table) VALUES (?, ?, ?, ?)")) {
				PGobject uuid = new PGobject();
				uuid.setType("uuid");
				uuid.setValue(id);
				ps.setObject(1, uuid);
				ps.setInt(2, EtlStatus.CREATED);
				java.util.Date created = new java.util.Date();
				ps.setTimestamp(3, new Timestamp(created.getTime()));
				ps.setString(4, dataTable);
				log.debug("Executing: " + ps);
				ps.execute();
			}

			try (PreparedStatement ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + dataTable + " (rownr SERIAL PRIMARY KEY, data JSONB NOT NULL)")) {
				log.debug("Executing: " + ps);
				ps.execute();
			}

			conn.commit();
			log.info("Created dataset " + id);
			return new PgDataset(getRowStore(), id, EtlStatus.CREATED, new java.util.Date(), dataTable);
		} catch (SQLException e) {
			log.error(e.getMessage());
		} finally {
			log.debug("Creating dataset took {} ms", System.currentTimeMillis() - before);
		}

		return null;
	}

	/**
	 * @see Datasets#purgeDataset(String)
	 */
	@Override
	public boolean purgeDataset(String id) {
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		long before = System.currentTimeMillis();
		try (Connection conn = getRowStore().getConnection()) {
			conn.setAutoCommit(false);

			try (PreparedStatement ps = conn.prepareStatement("DROP TABLE " + constructDataTableName(id))) {
				log.debug("Executing: " + ps);
				ps.execute();
			}

			try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + DATASETS_TABLE_NAME + " WHERE id = ?")) {
				PGobject uuid = new PGobject();
				uuid.setType("uuid");
				uuid.setValue(id);
				ps.setObject(1, uuid);
				log.debug("Executing: " + ps);
				ps.execute();
			}

			conn.commit();
			log.info("Purged dataset " + id);
			return true;
		} catch (SQLException e) {
			log.error(e.getMessage());
			return false;
		} finally {
			log.debug("Purging dataset took {} ms", System.currentTimeMillis() - before);
		}
	}

	/**
	 * @see Datasets#getDataset(String)
	 */
	@Override
	public Dataset getDataset(String id) {
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		try {
			return new PgDataset(rowstore, id);
		} catch (IllegalArgumentException | IllegalStateException iae) {
			log.error("Unable to load dataset with ID " + id);
			return null;
		}
	}

	/**
	 * @see Datasets#hasDataset(String)
	 */
	@Override
	public boolean hasDataset(String id) {
		long before = System.currentTimeMillis();
		try (Connection conn = rowstore.getConnection();
			 PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + PgDatasets.DATASETS_TABLE_NAME + " WHERE id = ?")) {
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(id);
			stmt.setObject(1, uuid);
			log.debug("Executing: " + stmt);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					return true;
				}
			}
		} catch (SQLException e) {
			if ("22P02".equals(e.getSQLState())) {
				log.debug("Probable alias detected: {}", e.getMessage());
			} else {
				SqlExceptionLogUtil.error(log, e);
			}
		} finally {
			log.debug("Checking for dataset existance took {} ms", System.currentTimeMillis() - before);
		}

		return false;
	}

	/**
	 * @return Returns the RowStore instance.
	 */
	public PgRowStore getRowStore() {
		return this.rowstore;
	}

	/**
	 * @see Datasets#amount()
	 */
	@Override
	public int amount() {
		long before = System.currentTimeMillis();
		int result = -1;
		try (Connection conn = getRowStore().getConnection();
			 PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) AS amount FROM " + DATASETS_TABLE_NAME);
			 ResultSet rs = stmt.executeQuery()) {
			log.debug("Executing: " + stmt);
			if (rs.next()) {
				result = rs.getInt("amount");
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			log.debug("Fetching amount of datasets took {} ms", System.currentTimeMillis() - before);
		}

		return result;
	}

	/**
	 * @see Datasets#createUniqueDatasetId()
	 */
	@Override
	public String createUniqueDatasetId() {
		String uuid;
		do {
			uuid = UUID.randomUUID().toString();
		} while (getRowStore().getDatasets().hasDataset(uuid));
		return uuid;
	}

	/**
	 * Constructs a name for a DB data table based on a supplied ID.
	 * @param id The ID to be used for constructing the table name.
	 * @return Returns a table name for storing a dataset's data.
	 */
	static final String DATA_TABLE_PATTERN = "data_[a-f0-9]{32}";

	private String constructDataTableName(String id) {
		String tableName = "data_" + id.replaceAll("-", "");
		if (!tableName.matches(DATA_TABLE_PATTERN)) {
			throw new IllegalArgumentException("Generated data table name does not match expected pattern: " + tableName);
		}
		return tableName;
	}

	/**
	 * @return Returns the length of the data table names.
	 */
	private int getDataTableNameLength() {
		return constructDataTableName(UUID.randomUUID().toString()).length();
	}

}
