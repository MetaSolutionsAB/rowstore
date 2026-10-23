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
import org.entrystore.rowstore.config.StartupRetry;
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
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.SQLExceptionTranslator;
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
	 * Data sources for health and status checks: the query budgets, with a short wait for a free connection.
	 */
	private final LimitedDataSource primaryProbe;

	private final LimitedDataSource queryProbe;

	/**
	 * The budgets per database block and budget name, for reporting their usage.
	 */
	private final Map<String, Map<String, LimitedDataSource>> budgets = new LinkedHashMap<>();

	/**
	 * Maximum time health and status checks wait for a free connection, see {@link #getProbeConnection(boolean)}.
	 */
	static final long PROBE_WAIT_MILLIS = 2000;

	/**
	 * SQL state of a lock that could not be acquired within lock_timeout.
	 */
	static final String LOCK_NOT_AVAILABLE = "55P03";

	/**
	 * SQL state of a statement that was cancelled, e.g., because of the statement timeout (querytimeout).
	 */
	static final String QUERY_CANCELED = "57014";

	private final JdbcTemplate jdbcTemplate;

	private final JdbcTemplate queryJdbcTemplate;

	private final JdbcTemplate loadJdbcTemplate;

	private final JdbcTemplate probeJdbcTemplate;

	private Datasets datasets;

	private final EtlProcessor etlProcessor;

	private final RowStoreConfig config;

	private volatile boolean shuttingDown;

	/**
	 * Name of the cleanup of column names (ETL-11) in the table of maintenance tasks.
	 */
	public static final String COLUMN_NAME_CLEANUP_TASK = "column-name-cleanup";

	private final MaintenanceTasks maintenanceTasks;

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
		primaryQuerySlots = addBudget("database", "query", datasource, primaryDb.getMaxQueryConnections(), primaryDb, true);
		if (separateQueryDatabase) {
			querySlots = addBudget("queryDatabase", "query", queryDatasource, queryDb.getMaxQueryConnections(), queryDb, true);
			exportSlots = addBudget("queryDatabase", "export", queryDatasource, queryDb.getMaxExportConnections(), queryDb, false);
		} else {
			querySlots = primaryQuerySlots;
			exportSlots = addBudget("database", "export", datasource, primaryDb.getMaxExportConnections(), primaryDb, false);
		}
		// every ETL job holds at most one load connection, so loads never wait for a load connection
		loadSlots = addBudget("database", "load", datasource, etlProcessor.getMaxEtlProcesses(), primaryDb, false);
		checkPoolSize(primaryDb, "database");
		if (separateQueryDatabase) {
			checkPoolSize(queryDb, "queryDatabase");
		}
		primaryProbe = probe(primaryQuerySlots, primaryDb);
		queryProbe = separateQueryDatabase ? probe(querySlots, queryDb) : primaryProbe;

		this.jdbcTemplate = queryTemplate(primaryQuerySlots);
		this.queryJdbcTemplate = queryTemplate(querySlots);
		this.loadJdbcTemplate = new JdbcTemplate(loadSlots);
		this.probeJdbcTemplate = queryTemplate(primaryProbe);
		// like the other startup maintenance below, on the primary database within its query budget
		this.maintenanceTasks = new MaintenanceTasks(jdbcTemplate);

		// like the migration in DataSourceConfig, the startup maintenance waits for the database if it is not reachable
		StartupRetry startupRetry = new StartupRetry();
		runStartupMaintenance(startupRetry, "reset datasets with interrupted loads or lost uploads", this::resetInterruptedLoadsOrThrow);
		runStartupMaintenance(startupRetry, "drop leftover shadow tables", this::dropLeftoverShadowTablesOrThrow);
		log.info("Started RowStore {}", versionInfo.getVersion());
	}

	/**
	 * Runs a step of the startup maintenance, retrying it while the database is not reachable (ARCH-7.08). Other
	 * failures are logged, the startup continues.
	 */
	private static void runStartupMaintenance(StartupRetry startupRetry, String description, StartupRetry.Task<?> task) {
		try {
			startupRetry.call(description, task);
		} catch (DataAccessException e) {
			log.error("Unable to {}: {}", description, e.getMessage());
		}
	}

	/**
	 * Creates a budget.
	 *
	 * @param queryBudget Whether the budget is for short operations.
	 */
	private LimitedDataSource addBudget(String dbName, String budgetName, DataSource target, int maxConnections,
										RowStoreConfig.Database db, boolean queryBudget) {
		long waitMillis = TimeUnit.SECONDS.toMillis(db.getConnectionWaitTimeout());
		LimitedDataSource budget = new LimitedDataSource(target, dbName + " " + budgetName, maxConnections, waitMillis,
				sessionSql(db, queryBudget));
		budgets.computeIfAbsent(dbName, k -> new LinkedHashMap<>()).put(budgetName, budget);
		long lockTimeout = db.getQueryLockTimeoutMillis();
		log.info("Limiting {} connections to {} (wait timeout {} s{})", budget.getName(), maxConnections,
				db.getConnectionWaitTimeout(), queryBudget ? ", lock timeout " + (lockTimeout > 0 ? lockTimeout + " ms" : "not set") : "");
		return budget;
	}

	/**
	 * Connections of query budgets wait at most queryLockTimeout (default half the wait timeout) for a lock
	 * (ARCH-7.03), so that a request for a dataset whose data table is locked, e.g., by a replace waiting for a long
	 * export, frees its connection in time for the requests waiting behind it. Connections of other budgets keep the
	 * default lock_timeout: connections of a HikariCP pool may have been used by a query budget before, so it is reset
	 * for them; without a pool, every connection is a new session (assuming direct connections to PostgreSQL, see
	 * ARCH-7.06 for PgBouncer), so nothing has to be reset. If queryLockTimeout is 0, no session state is changed at all.
	 *
	 * @return Returns the statement to execute on every connection of the budget, or null.
	 */
	static String sessionSql(RowStoreConfig.Database db, boolean queryBudget) {
		long lockTimeout = db.getQueryLockTimeoutMillis();
		if (lockTimeout <= 0) {
			return null;
		}
		if (queryBudget) {
			return "SET lock_timeout = " + lockTimeout;
		}
		return db.getConnectionPoolMax() > 0 ? "RESET lock_timeout" : null;
	}

	private static LimitedDataSource probe(LimitedDataSource querySlots, RowStoreConfig.Database db) {
		return querySlots.withWaitTimeout(Math.min(PROBE_WAIT_MILLIS, TimeUnit.SECONDS.toMillis(db.getConnectionWaitTimeout())));
	}

	/**
	 * Creates a JdbcTemplate for short operations: statements are limited by querytimeout (if configured), and lock and
	 * statement timeouts are translated to {@link TransientDataAccessResourceException}, which results in 503.
	 */
	private JdbcTemplate queryTemplate(DataSource budget) {
		JdbcTemplate template = new JdbcTemplate(budget);
		if (config.getQueryTimeout() > 0) {
			template.setQueryTimeout(config.getQueryTimeout());
		}
		SQLExceptionTranslator translator = template.getExceptionTranslator();
		template.setExceptionTranslator((task, sql, e) -> isTemporarilyUnavailable(e) ?
				new TransientDataAccessResourceException(task + " failed: " + e.getMessage(), e) :
				translator.translate(task, sql, e));
		return template;
	}

	/**
	 * @return Returns whether the exception is a lock timeout or a cancelled statement (e.g., statement timeout).
	 */
	static boolean isTemporarilyUnavailable(SQLException e) {
		return LOCK_NOT_AVAILABLE.equals(e.getSQLState()) || QUERY_CANCELED.equals(e.getSQLState());
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
	 * Sets the status of all datasets in PROCESSING or ACCEPTED_DATA to ERROR (ETL-8.06). Called at startup: since a
	 * database must not be used by more than one RowStore instance and the queue of uploads is kept in memory, such
	 * datasets were left behind by loads that were interrupted (e.g., by a crash) or by uploads that were still queued
	 * when RowStore stopped and are lost. The previous data of the datasets is still served.
	 *
	 * @return Returns the IDs of the datasets that were reset.
	 */
	public List<String> resetInterruptedLoads() {
		try {
			return resetInterruptedLoadsOrThrow();
		} catch (DataAccessException e) {
			log.error("Unable to reset datasets with interrupted loads or lost uploads: {}", e.getMessage());
			return List.of();
		}
	}

	/**
	 * Like {@link #resetInterruptedLoads()}.
	 *
	 * @throws DataAccessException If the database could not be accessed.
	 */
	private List<String> resetInterruptedLoadsOrThrow() {
		List<String> ids = new ArrayList<>();
		ids.addAll(resetStatus(EtlStatus.PROCESSING, "their loads were interrupted"));
		ids.addAll(resetStatus(EtlStatus.ACCEPTED_DATA, "their queued uploads were lost when RowStore stopped"));
		return ids;
	}

	/**
	 * Sets the status of all datasets in the given status to ERROR.
	 *
	 * @return Returns the IDs of the datasets that were reset.
	 * @throws DataAccessException If the database could not be accessed.
	 */
	private List<String> resetStatus(int status, String reason) {
		List<String> ids = jdbcTemplate.queryForList(
				"UPDATE " + PgDatasets.DATASETS_TABLE_NAME + " SET status = ? WHERE status = ? RETURNING id::text",
				String.class, EtlStatus.ERROR, status);
		if (!ids.isEmpty()) {
			log.warn("Set status of {} dataset(s) from {} to {} because {}: {}", ids.size(),
					EtlStatus.toString(status), EtlStatus.toString(EtlStatus.ERROR), reason, ids);
		}
		return ids;
	}

	/**
	 * Drops shadow tables of replaces (see PgDataset#populate(File, boolean)) that exist under their own name,
	 * i.e., that were left behind. Called at startup. A replace never commits its shadow table under its own name,
	 * so this is a safety net that only touches tables matching {@link PgDatasets#SHADOW_TABLE_PATTERN}.
	 *
	 * @return Returns the names of the dropped tables.
	 */
	public List<String> dropLeftoverShadowTables() {
		try {
			return dropLeftoverShadowTablesOrThrow();
		} catch (DataAccessException e) {
			log.error("Unable to drop leftover shadow tables: {}", e.getMessage());
			return List.of();
		}
	}

	/**
	 * Like {@link #dropLeftoverShadowTables()}.
	 *
	 * @throws DataAccessException If the database could not be accessed.
	 */
	private List<String> dropLeftoverShadowTablesOrThrow() {
		List<String> dropped = new ArrayList<>();
		List<String> tables = jdbcTemplate.queryForList(
				"SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename ~ ?",
				String.class, "^" + PgDatasets.SHADOW_TABLE_PATTERN + "$");
		for (String table : tables) {
			if (table.matches(PgDatasets.SHADOW_TABLE_PATTERN)) {
				jdbcTemplate.execute("DROP TABLE IF EXISTS " + table);
				dropped.add(table);
			}
		}
		if (!dropped.isEmpty()) {
			log.warn("Dropped {} leftover shadow table(s): {}", dropped.size(), dropped);
		}
		return dropped;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void onApplicationReady() {
		startColumnNameCleanup();
	}

	/**
	 * Starts the cleanup of column names (see {@link #cleanColumnNames()}) in the background, unless a run of it has
	 * completed before (ETL-11.07).
	 *
	 * @return Returns the thread of the cleanup, or null if it was skipped.
	 */
	public Thread startColumnNameCleanup() {
		if (maintenanceTasks.isCompleted(COLUMN_NAME_CLEANUP_TASK)) {
			log.info("Column names were cleaned completely before, skipping the cleanup");
			return null;
		}
		return Thread.ofVirtual().name("column-name-cleanup").start(() -> {
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
	 * <p>
	 * If all datasets were checked and none of them failed (datasets skipped because of colliding names count as
	 * checked, since a repeated run cannot clean them either), the run is recorded as completed, so that later startups
	 * skip the cleanup (ETL-11.07). New uploads always store clean names.
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
		boolean complete = true;
		for (Dataset d : all) {
			PgDataset dataset = (PgDataset) d;
			if (shuttingDown) {
				log.warn("Stopped cleaning column names, RowStore is shutting down");
				complete = false;
				break;
			}
			try {
				if (!dataset.hasUncleanColumnNames()) {
					continue;
				}
				Map<String, String> renames = etlProcessor.callExclusively(dataset, dataset::cleanColumnNames).join();
				if (renames == null) {
					// the data table could not be locked, the dataset is cleaned at a later startup
					complete = false;
				} else if (!renames.isEmpty()) {
					cleaned.add(dataset.getCanonicalId());
				}
			} catch (CancellationException | RejectedExecutionException e) {
				log.warn("Stopped cleaning column names, the ETL processor is shut down");
				complete = false;
				break;
			} catch (SQLException | RuntimeException e) {
				complete = false;
				Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
				if (LimitedDataSource.isWaitTimeout(cause) || cause instanceof TransientDataAccessResourceException ||
						(cause instanceof SQLException se && isTemporarilyUnavailable(se))) {
					// the database is in use (no connection or lock available in time), not unreachable; the dataset is
					// cleaned at the next startup or by an append
					log.warn("Skipped cleaning column names of dataset {}: {}", dataset.getId(), cause.getMessage());
					continue;
				}
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
		if (complete) {
			maintenanceTasks.markCompleted(COLUMN_NAME_CLEANUP_TASK);
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
	 * @see RowStore#getProbeConnection(boolean)
	 */
	@Override
	public Connection getProbeConnection(boolean queryDatabase) {
		return connect(queryDatabase ? queryProbe : primaryProbe);
	}

	/**
	 * An ETL job writes the status of its dataset while it does not hold its load connection, so a load connection is
	 * always free (ARCH-7.02).
	 *
	 * @see RowStore#getLoadJdbcTemplate()
	 */
	@Override
	public JdbcTemplate getLoadJdbcTemplate() {
		return loadJdbcTemplate;
	}

	/**
	 * @return Returns a JdbcTemplate for health and status checks (query budget of the primary database with a short
	 * wait for a free connection).
	 */
	JdbcTemplate getProbeJdbcTemplate() {
		return probeJdbcTemplate;
	}

	/**
	 * @throws CannotGetJdbcConnectionException If no connection could be obtained, like JdbcTemplate.
	 */
	private static Connection connect(DataSource budget) {
		try {
			return budget.getConnection();
		} catch (SQLException e) {
			// unlike the message of SQLException, the message of DataAccessException does not include the cause
			throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection: " + e.getMessage(), e);
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
