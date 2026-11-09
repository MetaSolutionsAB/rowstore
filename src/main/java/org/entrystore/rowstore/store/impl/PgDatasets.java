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

import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.Datasets;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.core.ConnectionCallback;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * A PostgreSQL-specific implementation of the Datasets interface.
 *
 * @author Hannes Ebner
 * @see Datasets
 */
public class PgDatasets implements Datasets {

	private static final Logger log = LoggerFactory.getLogger(PgDatasets.class);

	protected static final String DATASETS_TABLE_NAME = "datasets";

	protected static final String ALIAS_TABLE_NAME = "aliases";

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
		try {
			List<Dataset> datasets = rowstore.getJdbcTemplate().query(
					"SELECT * FROM " + DATASETS_TABLE_NAME,
					(rs, rowNum) -> new PgDataset(rowstore,
							((UUID) rs.getObject("id")).toString(),
							rs.getInt("status"),
							rs.getTimestamp("created"),
							rs.getString("data_table")));
			return new HashSet<>(datasets);
		} catch (DataAccessResourceFailureException | TransientDataAccessResourceException e) {
			// e.g., no connection available within the wait timeout (503)
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			return null;
		} finally {
			log.debug("Fetching datasets took {} ms", System.currentTimeMillis() - before);
		}
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

			createDataTable(conn, dataTable);

			conn.commit();
			log.info("Created dataset " + id);
			return new PgDataset(getRowStore(), id, EtlStatus.CREATED, new java.util.Date(), dataTable);
		} catch (SQLException e) {
			if (PgRowStore.isTemporarilyUnavailable(e)) {
				// e.g., the datasets table is locked for longer than the lock timeout (503, ARCH-7.03)
				throw new TransientDataAccessResourceException("Unable to create dataset: " + e.getMessage(), e);
			}
			log.error(e.getMessage());
		} finally {
			log.debug("Creating dataset took {} ms", System.currentTimeMillis() - before);
		}

		return null;
	}

	/**
	 * Creates a table for storing a dataset's rows. All data tables, including shadow tables, must be created with
	 * this method to guarantee an identical schema.
	 *
	 * @param conn The connection to use. Transaction handling is left to the caller.
	 * @param tableName The name of the data table, must match {@link #DATA_TABLE_PATTERN} or
	 *                  {@link #SHADOW_TABLE_PATTERN}.
	 * @throws SQLException If the table could not be created.
	 */
	static void createDataTable(Connection conn, String tableName) throws SQLException {
		if (tableName == null || !(tableName.matches(DATA_TABLE_PATTERN) || tableName.matches(SHADOW_TABLE_PATTERN))) {
			throw new IllegalArgumentException("Data table name does not match expected pattern: " + tableName);
		}
		try (PreparedStatement ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + " (rownr BIGSERIAL PRIMARY KEY, data JSONB NOT NULL)")) {
			log.debug("Executing: " + ps);
			ps.execute();
		}
	}

	/**
	 * Deletes the dataset's row in the datasets table and its aliases in one transaction on the query budget of the
	 * primary database, and then submits a job of the dataset to the ETL processor that drops its data table (see
	 * {@link PgDataset#dropTablesInBackground()}). The transaction takes only row locks, so it does not wait for
	 * readers of the data table; it waits at most queryLockTimeout for a concurrent change of the dataset's row (e.g.,
	 * a status change or an alias update), and its statements are limited by querytimeout. The status condition is
	 * part of the DELETE, so an upload accepted concurrently either sets ACCEPTED_DATA first (LOCKED) or finds no
	 * dataset anymore (its load is skipped, see PgDataset#populate(java.io.File, boolean)).
	 *
	 * @see Datasets#purgeDataset(String)
	 */
	@Override
	public PurgeResult purgeDataset(String id) {
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		long before = System.currentTimeMillis();
		try {
			// lock and statement timeouts are translated to TransientDataAccessResourceException (503), connection
			// failures to DataAccessResourceFailureException (503), other failures to other DataAccessExceptions (500)
			Purge purge = rowstore.getJdbcTemplate().execute((ConnectionCallback<Purge>) conn -> {
				conn.setAutoCommit(false);
				try {
					Purge result = deleteRegistryRows(conn, id);
					if (result.result() == PurgeResult.PURGED) {
						conn.commit();
					} else {
						conn.rollback();
					}
					return result;
				} catch (SQLException | RuntimeException e) {
					try {
						conn.rollback();
					} catch (SQLException e1) {
						e.addSuppressed(e1);
					}
					throw e;
				}
			});
			if (purge.result() == PurgeResult.PURGED) {
				log.info("Deleted dataset {} and {} alias(es), its data table {} is dropped in the background",
						purge.id(), purge.aliases(), purge.dataTable());
				submitDropOfDataTable(purge.id(), purge.dataTable());
			}
			return purge.result();
		} finally {
			log.debug("Purging dataset took {} ms", System.currentTimeMillis() - before);
		}
	}

	/**
	 * Outcome of the deletion of a dataset's registry rows.
	 *
	 * @param id The canonical ID of the deleted dataset, null unless PURGED.
	 * @param dataTable The name of its data table, null unless PURGED.
	 * @param aliases The number of deleted aliases.
	 */
	private record Purge(PurgeResult result, String id, String dataTable, int aliases) {
	}

	/**
	 * Deletes the dataset's row in the datasets table if its status is AVAILABLE or ERROR, and its aliases, in the
	 * caller's transaction. The caller commits only if the result is PURGED.
	 */
	private Purge deleteRegistryRows(Connection conn, String id) throws SQLException {
		String canonicalId;
		String storedTable;
		try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + DATASETS_TABLE_NAME +
				" WHERE id = ?::uuid AND status IN (?, ?) RETURNING id::text, data_table")) {
			ps.setString(1, id);
			ps.setInt(2, EtlStatus.AVAILABLE);
			ps.setInt(3, EtlStatus.ERROR);
			log.debug("Executing: " + ps);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					canonicalId = null;
					storedTable = null;
				} else {
					canonicalId = rs.getString(1);
					storedTable = rs.getString(2);
				}
			}
		}
		if (canonicalId == null) {
			try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM " + DATASETS_TABLE_NAME + " WHERE id = ?::uuid")) {
				ps.setString(1, id);
				try (ResultSet rs = ps.executeQuery()) {
					return new Purge(rs.next() ? PurgeResult.LOCKED : PurgeResult.NOT_FOUND, null, null, 0);
				}
			}
		}
		int aliases;
		try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + ALIAS_TABLE_NAME + " WHERE dataset_id = ?::uuid")) {
			ps.setString(1, canonicalId);
			log.debug("Executing: " + ps);
			aliases = ps.executeUpdate();
		}
		String dataTable = storedTable != null ? storedTable.trim() : null;
		if (dataTable == null || !dataTable.matches(DATA_TABLE_PATTERN)) {
			// a corrupt row (API-6.06); the table named after the ID is the only one the dataset can have had
			dataTable = constructDataTableName(canonicalId);
			log.error("Dataset {} has an invalid data table name in the datasets table, dropping {} instead", canonicalId, dataTable);
		}
		return new Purge(PurgeResult.PURGED, canonicalId, dataTable, aliases);
	}

	/**
	 * Submits the drop of the data table of a deleted dataset as jobs of the dataset (ETL-12.01), so that they run
	 * after the dataset's earlier jobs and never concurrently with one of them. If they cannot run because RowStore
	 * is shutting down, the table is dropped at the next startup (ETL-12.03).
	 */
	private void submitDropOfDataTable(String id, String dataTable) {
		new PgDataset(rowstore, id, EtlStatus.ERROR, null, dataTable).dropTablesInBackground();
	}

	/**
	 * Returns an SQL condition that is true if a row of the datasets table references a data table, given as an SQL
	 * expression of type text that is used twice (e.g., a parameter {@code CAST(? AS text)}, which then has to be set
	 * twice). A data table is referenced by the stored name (trimmed, since data_table is CHAR(37), and in lower case,
	 * since PostgreSQL folds unquoted identifiers, so that 1.x served stored names in upper case) and by the name
	 * derived from the dataset's ID (DOM-6.01), which is the table of a dataset whose data_table is NULL or invalid.
	 * Tables referenced either way must never be dropped as leftovers (ETL-12.01, ETL-12.03).
	 */
	static String referencedByDataset(String tableExpression) {
		return "EXISTS (SELECT 1 FROM " + DATASETS_TABLE_NAME + " d WHERE lower(trim(d.data_table)) = " + tableExpression +
				" OR 'data_' || replace(d.id::text, '-', '') = " + tableExpression + ")";
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
		} catch (IllegalArgumentException | IllegalStateException e) {
			// an unknown or malformed ID is a client error (404), already recorded by the access log (ROWS-169);
			// database failures are not caught here and propagate (503 or 500)
			log.debug("No dataset with ID {}: {}", id, e.getMessage());
			return null;
		}
	}

	/**
	 * @see Datasets#hasDataset(String)
	 */
	@Override
	public boolean hasDataset(String id) {
		long before = System.currentTimeMillis();
		try {
			Integer count = rowstore.getJdbcTemplate().queryForObject(
					"SELECT COUNT(*) FROM " + DATASETS_TABLE_NAME + " WHERE id = ?::uuid", Integer.class, id);
			return count != null && count > 0;
		} catch (DataAccessException e) {
			Throwable cause = e.getCause();
			if (cause instanceof SQLException se && "22P02".equals(se.getSQLState())) {
				log.debug("Probable alias detected: {}", se.getMessage());
			} else {
				log.error(e.getMessage());
			}
			return false;
		} finally {
			log.debug("Checking for dataset existance took {} ms", System.currentTimeMillis() - before);
		}
	}

	/**
	 * @return Returns the RowStore instance.
	 */
	public PgRowStore getRowStore() {
		return this.rowstore;
	}

	/**
	 * Used by health and status checks, so it waits only briefly for a free connection.
	 *
	 * @see Datasets#amount()
	 */
	@Override
	public int amount() {
		long before = System.currentTimeMillis();
		try {
			Integer result = rowstore.getProbeJdbcTemplate().queryForObject(
					"SELECT COUNT(*) FROM " + DATASETS_TABLE_NAME, Integer.class);
			return result != null ? result : -1;
		} catch (DataAccessResourceFailureException | TransientDataAccessResourceException e) {
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			return -1;
		} finally {
			log.debug("Fetching amount of datasets took {} ms", System.currentTimeMillis() - before);
		}
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
	 * Pattern of the shadow table into which a replace loads the new data of a dataset, before it replaces the
	 * data table. The name of a shadow table is never stored as a dataset's data table.
	 */
	static final String SHADOW_TABLE_PATTERN = "data_[a-f0-9]{32}_new";

	/**
	 * @param dataTable The name of a data table, must match {@link #DATA_TABLE_PATTERN}.
	 * @return Returns the name of the shadow table of a data table.
	 */
	static String shadowTableName(String dataTable) {
		if (dataTable == null || !dataTable.matches(DATA_TABLE_PATTERN)) {
			throw new IllegalArgumentException("Data table name does not match expected pattern: " + dataTable);
		}
		return dataTable + "_new";
	}

}
