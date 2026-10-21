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
import org.entrystore.rowstore.store.ConnectionUsage;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.Datasets;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.store.RowStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A PostgreSQL-specific implementation of the RowStore interface.
 *
 * @author Hannes Ebner
 * @see RowStore
 */
@Service
public class PgRowStore implements RowStore {

	private final static Logger log = LoggerFactory.getLogger(PgRowStore.class);

	/**
	 * Connection budgets (ARCH-7): short operations on the primary database and on the query database (the same
	 * budget if there is no separate query database), exports (on the query database) and loads (on the primary
	 * database).
	 */
	private final LimitedDataSource primaryQuerySlots;

	private final LimitedDataSource querySlots;

	private final LimitedDataSource exportSlots;

	private final LimitedDataSource loadSlots;

	/**
	 * The budgets per database block and budget name, for reporting their usage.
	 */
	private final Map<String, Map<String, LimitedDataSource>> budgets = new LinkedHashMap<>();

	private final JdbcTemplate jdbcTemplate;

	private final JdbcTemplate queryJdbcTemplate;

	private Datasets datasets;

	private final EtlProcessor etlProcessor;

	private final RowStoreConfig config;

	private volatile boolean shuttingDown;

	public PgRowStore(DataSource datasource,
					  @Qualifier("queryDataSource") DataSource queryDatasource,
					  RowStoreConfig config,
					  VersionInfo versionInfo) {
		if (config == null) {
			throw new IllegalArgumentException("Configuration must not be null");
		}
		this.config = config;
		etlProcessor = new EtlProcessor(this);

		RowStoreConfig.Database primaryDb = config.getDatabase();
		RowStoreConfig.Database queryDb = config.getQueryDatabase();
		boolean separateQueryDatabase = queryDb != primaryDb;
		primaryQuerySlots = addBudget("database", "query", datasource, primaryDb.getMaxQueryConnections(), primaryDb);
		if (separateQueryDatabase) {
			querySlots = addBudget("queryDatabase", "query", queryDatasource, queryDb.getMaxQueryConnections(), queryDb);
			exportSlots = addBudget("queryDatabase", "export", queryDatasource, queryDb.getMaxExportConnections(), queryDb);
		} else {
			querySlots = primaryQuerySlots;
			exportSlots = addBudget("database", "export", datasource, primaryDb.getMaxExportConnections(), primaryDb);
		}
		// every ETL job holds at most one load connection, so loads never wait for a load connection
		loadSlots = addBudget("database", "load", datasource, etlProcessor.getMaxEtlProcesses(), primaryDb);
		checkPoolSize(primaryDb, "database");
		if (separateQueryDatabase) {
			checkPoolSize(queryDb, "queryDatabase");
		}

		this.jdbcTemplate = new JdbcTemplate(primaryQuerySlots);
		this.queryJdbcTemplate = new JdbcTemplate(querySlots);

		resetInterruptedLoads();
		dropLeftoverShadowTables();
		log.info("Started RowStore {}", versionInfo.getVersion());
	}

	private LimitedDataSource addBudget(String dbName, String budgetName, DataSource target, int maxConnections,
										RowStoreConfig.Database db) {
		LimitedDataSource budget = new LimitedDataSource(target, dbName + " " + budgetName, maxConnections,
				TimeUnit.SECONDS.toMillis(db.getConnectionWaitTimeout()));
		budgets.computeIfAbsent(dbName, k -> new LinkedHashMap<>()).put(budgetName, budget);
		log.info("Limiting {} connections to {} (wait timeout {} s)", budget.getName(), maxConnections, db.getConnectionWaitTimeout());
		return budget;
	}

	/**
	 * Warns if the connection pool of a database (if enabled) is smaller than the sum of its connection budgets, since
	 * the budgets then compete for pooled connections and a connection may wait for the pool instead.
	 */
	private void checkPoolSize(RowStoreConfig.Database db, String dbName) {
		int sum = budgets.get(dbName).values().stream().mapToInt(LimitedDataSource::getMaxConnections).sum();
		if (db.getConnectionPoolMax() > 0 && db.getConnectionPoolMax() < sum) {
			log.warn("connectionPoolMax {} of {} is lower than the sum of its connection limits {}, " +
					"connections may have to wait for the pool", db.getConnectionPoolMax(), dbName, sum);
		}
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
	 * Drops shadow tables of replaces (see PgDataset#populate(File, boolean)) that exist under their own name,
	 * i.e., that were left behind. Called at startup. A replace never commits its shadow table under its own name,
	 * so this is a safety net that only touches tables matching {@link PgDatasets#SHADOW_TABLE_PATTERN}.
	 *
	 * @return Returns the names of the dropped tables.
	 */
	public List<String> dropLeftoverShadowTables() {
		List<String> dropped = new ArrayList<>();
		try {
			List<String> tables = jdbcTemplate.queryForList(
					"SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename ~ ?",
					String.class, "^" + PgDatasets.SHADOW_TABLE_PATTERN + "$");
			for (String table : tables) {
				if (table.matches(PgDatasets.SHADOW_TABLE_PATTERN)) {
					jdbcTemplate.execute("DROP TABLE IF EXISTS " + table);
					dropped.add(table);
				}
			}
		} catch (DataAccessException e) {
			log.error("Unable to drop leftover shadow tables: {}", e.getMessage());
		}
		if (!dropped.isEmpty()) {
			log.warn("Dropped {} leftover shadow table(s): {}", dropped.size(), dropped);
		}
		return dropped;
	}

	/**
	 * Starts the cleanup of column names (see {@link #cleanColumnNames()}) in the background after startup.
	 */
	@EventListener(ApplicationReadyEvent.class)
	public void startColumnNameCleanup() {
		Thread.ofVirtual().name("column-name-cleanup").start(() -> {
			try {
				cleanColumnNames();
			} catch (RuntimeException e) {
				log.error("Cleaning column names failed", e);
			}
		});
	}

	/**
	 * Renames the columns of datasets whose stored column names are not clean, e.g., because they were stored before
	 * 2.0 with a byte order mark or soft hyphens (see PgDataset#cleanColumnNames()). Called in the background after
	 * startup. Each dataset is renamed as a task that runs exclusively for the dataset in the order of its ETL jobs
	 * (EtlProcessor#callExclusively). Datasets are renamed one at a time, the next one only after the previous one has
	 * finished, to limit the load on the database. Datasets that are clean are not touched, so a repeated run renames
	 * nothing. The cleanup stops (logged once at WARN level) when RowStore shuts down or the database is not reachable;
	 * the remaining datasets are cleaned at the next startup or by an append (see PgDataset#populate).
	 *
	 * @return Returns the IDs of the datasets whose columns were renamed.
	 */
	public List<String> cleanColumnNames() {
		Set<Dataset> all = getDatasets().getAll();
		if (all == null) {
			log.error("Unable to clean column names, the datasets could not be listed");
			return List.of();
		}
		List<String> cleaned = new ArrayList<>();
		for (Dataset d : all) {
			PgDataset dataset = (PgDataset) d;
			if (shuttingDown) {
				log.warn("Stopped cleaning column names, RowStore is shutting down");
				break;
			}
			try {
				if (!dataset.hasUncleanColumnNames()) {
					continue;
				}
				Map<String, String> renames = etlProcessor.callExclusively(dataset, dataset::cleanColumnNames).join();
				if (!renames.isEmpty()) {
					cleaned.add(dataset.getCanonicalId());
				}
			} catch (CancellationException | RejectedExecutionException e) {
				log.warn("Stopped cleaning column names, the ETL processor is shut down");
				break;
			} catch (SQLException | RuntimeException e) {
				Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
				if (isConnectionFailure(cause)) {
					log.warn("Stopped cleaning column names, the database is not reachable: {}", cause.getMessage());
					break;
				}
				log.error("Unable to clean column names of dataset {}", dataset.getId(), cause);
			}
		}
		if (!cleaned.isEmpty()) {
			log.info("Cleaned column names of {} dataset(s): {}", cleaned.size(), cleaned);
		}
		return cleaned;
	}

	/**
	 * @return Returns whether the throwable or one of its causes indicates that the database is not reachable.
	 */
	static boolean isConnectionFailure(Throwable t) {
		for (Throwable c = t; c != null; c = c.getCause()) {
			if (c instanceof DataAccessResourceFailureException || c instanceof SQLTransientConnectionException ||
					c instanceof SQLNonTransientConnectionException ||
					(c instanceof SQLException se && se.getSQLState() != null && se.getSQLState().startsWith("08"))) {
				return true;
			}
		}
		return false;
	}

	/**
	 * @see RowStore#getConnection()
	 */
	@Override
	public Connection getConnection() {
		return connect(primaryQuerySlots);
	}

	/**
	 * @see RowStore#getQueryConnection()
	 */
	@Override
	public Connection getQueryConnection() {
		return connect(querySlots);
	}

	/**
	 * @see RowStore#getExportConnection()
	 */
	@Override
	public Connection getExportConnection() {
		return connect(exportSlots);
	}

	/**
	 * @see RowStore#getLoadConnection()
	 */
	@Override
	public Connection getLoadConnection() {
		return connect(loadSlots);
	}

	/**
	 * @throws CannotGetJdbcConnectionException If no connection could be obtained, like JdbcTemplate.
	 */
	private static Connection connect(DataSource budget) {
		try {
			return budget.getConnection();
		} catch (SQLException e) {
			throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", e);
		}
	}

	/**
	 * @see RowStore#getConnectionUsage()
	 */
	@Override
	public Map<String, Map<String, ConnectionUsage>> getConnectionUsage() {
		Map<String, Map<String, ConnectionUsage>> result = new LinkedHashMap<>();
		budgets.forEach((dbName, dbBudgets) -> {
			Map<String, ConnectionUsage> usage = new LinkedHashMap<>();
			dbBudgets.forEach((budgetName, budget) -> usage.put(budgetName, budget.getUsage()));
			result.put(dbName, usage);
		});
		return result;
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
		synchronized (this) {
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
		shuttingDown = true;
		etlProcessor.shutdown();
		log.info("Shutdown complete");
	}

}
