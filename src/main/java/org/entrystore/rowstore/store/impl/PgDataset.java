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

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.lang3.StringUtils;
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.QueryResult;
import org.entrystore.rowstore.store.RowStore;
import org.entrystore.rowstore.util.DatasetUtil;
import org.entrystore.rowstore.util.Hashing;
import org.json.JSONException;
import org.json.JSONObject;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A PostgreSQL-specific implementation of the Dataset interface.
 *
 * @author Hannes Ebner
 * @see Dataset
 */
public class PgDataset implements Dataset {

	private static final Logger log = LoggerFactory.getLogger(PgDataset.class);

	private String id;

	private String canonicalId;

	private int status;

	private Date created;

	private String dataTable;

	private final RowStore rowstore;

	private final Map<String, Integer> columnSize = new HashMap<>();

	private final int maxSizeForIndex = 256;

	/**
	 * Delays between the attempts to write the final status of a load: six attempts over about 1.5 minutes.
	 */
	private long[] statusRetryDelaysMillis = {1000, 5000, 15000, 30000, 45000};

	/**
	 * Maximum time an attempt to lock the data table for a replace waits, except the last attempt, which waits
	 * without a timeout, see lockDataTableForReplace().
	 */
	static final String SWAP_LOCK_TIMEOUT = "3s";

	/**
	 * Delays between the attempts to lock the data table for a replace: four attempts with a timeout and a last one
	 * without. The delays are short because the load's transaction is idle meanwhile (idle_in_transaction_session_timeout).
	 */
	private long[] swapRetryDelaysMillis = {1000, 2000, 5000, 5000};

	/**
	 * SQL state of a lock that could not be acquired within lock_timeout.
	 */
	private static final String LOCK_NOT_AVAILABLE = "55P03";

	protected PgDataset(RowStore rowstore, String id) {
		if (rowstore == null) {
			throw new IllegalArgumentException("RowStore must not be null");
		}
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}

		this.rowstore = rowstore;
		this.id = id;

		// we check whether id is an alias and we try to resolve
		String resolvedAlias = resolveAlias(id);
		if (resolvedAlias != null) {
			this.id = resolvedAlias;
		} else if (this.id.length() < 36 || !DatasetUtil.isUUID(this.id)) {
			throw new IllegalArgumentException("Dataset ID must be a valid UUID with a length of 36 characters");
		}

		initFromDb();
	}

	protected PgDataset(RowStore rowstore, String id, int status, Date created, String dataTable) {
		if (rowstore == null) {
			throw new IllegalArgumentException("RowStore must not be null");
		}
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		if (dataTable != null) {
			validateDataTableName(dataTable.trim());
		}
		this.rowstore = rowstore;
		this.id = id;
		this.canonicalId = id;
		this.status = status;
		this.created = created;
		this.dataTable = dataTable;
	}

	/**
	 * @see Dataset#getId()
	 */
	@Override
	public String getId() {
		return id;
	}

	/**
	 * @see Dataset#getCanonicalId()
	 */
	@Override
	public String getCanonicalId() {
		return canonicalId;
	}

	/**
	 * @see Dataset#getStatus()
	 */
	@Override
	public int getStatus() {
		// we reload the info from the DB because the status may have changed
		initFromDb();
		return status;
	}

	/**
	 * @see Dataset#setStatus(int)
	 */
	@Override
	public void setStatus(int status) {
		try {
			writeStatus(status);
		} catch (DataAccessException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * Writes the status to the database. The status field is only updated if this succeeds, so that it is the last
	 * status known to be stored.
	 *
	 * @throws DataAccessException If the status could not be written.
	 */
	private void writeStatus(int status) {
		long before = System.currentTimeMillis();
		try {
			log.info("Setting status of {} to {}({})", getId(), EtlStatus.toString(status), status);
			rowstore.getJdbcTemplate().update(
					"UPDATE " + PgDatasets.DATASETS_TABLE_NAME + " SET status = ? WHERE id = ?::uuid",
					status, id);
			this.status = status;
		} finally {
			log.debug("Setting status took {} ms", System.currentTimeMillis() - before);
		}
	}

	/**
	 * Writes the status to the database and retries with backoff if this fails, e.g., because the database is
	 * temporarily unreachable. Used for the final status of a load, which must not be lost.
	 *
	 * @return Returns true if the status was written.
	 */
	private boolean writeStatusWithRetry(int status) {
		for (int attempt = 0; ; attempt++) {
			try {
				writeStatus(status);
				return true;
			} catch (DataAccessException e) {
				if (attempt >= statusRetryDelaysMillis.length) {
					log.error("Giving up setting status of {} to {}({}) after {} attempts",
							getId(), EtlStatus.toString(status), status, attempt + 1, e);
					return false;
				}
				long delay = statusRetryDelaysMillis[attempt];
				log.warn("Unable to set status of {} to {}({}), retrying in {} ms: {}",
						getId(), EtlStatus.toString(status), status, delay, e.getMessage());
				try {
					Thread.sleep(delay);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					log.error("Interrupted while retrying to set status of {} to {}({})", getId(), EtlStatus.toString(status), status);
					return false;
				}
			}
		}
	}

	/**
	 * Sets the delays between the attempts to write the final status of a load. Intended for tests.
	 */
	void setStatusRetryDelays(long... delaysMillis) {
		this.statusRetryDelaysMillis = delaysMillis.clone();
	}

	/**
	 * Sets the status to PROCESSING with a conditional update. The caller must run the load as the dataset's
	 * only job in this RowStore instance (see EtlProcessor), and a database must not be used by more than one
	 * RowStore instance, so a dataset that already is in PROCESSING is stale (e.g., because a load was interrupted
	 * by a database outage) and is taken over.
	 *
	 * @throws DataAccessException If the status could not be written.
	 */
	private void markAsProcessing() {
		int updated = rowstore.getJdbcTemplate().update(
				"UPDATE " + PgDatasets.DATASETS_TABLE_NAME + " SET status = ? WHERE id = ?::uuid AND status <> ?",
				EtlStatus.PROCESSING, id, EtlStatus.PROCESSING);
		if (updated == 0) {
			log.warn("Dataset {} is already in status {}({}) without an active load (or it does not exist anymore), taking it over",
					getId(), EtlStatus.toString(EtlStatus.PROCESSING), EtlStatus.PROCESSING);
		} else {
			log.info("Setting status of {} to {}({})", getId(), EtlStatus.toString(EtlStatus.PROCESSING), EtlStatus.PROCESSING);
		}
		this.status = EtlStatus.PROCESSING;
	}

	/**
	 * @see Dataset#getCreationDate()
	 */
	@Override
	public Date getCreationDate() {
		return created;
	}

	private static void validateDataTableName(String name) {
		if (!name.matches(PgDatasets.DATA_TABLE_PATTERN)) {
			throw new IllegalArgumentException("Invalid data table name: " + name);
		}
	}

	private String getDataTable() {
		return dataTable;
	}

	/**
	 * @see Dataset#populate(File, boolean)
	 */
	@Override
	public boolean populate(File csvFile, boolean append) throws IOException {
		if (csvFile == null) {
			throw new IllegalArgumentException("Argument must not be null");
		}

		// The outcome is tracked locally: the final status must not depend on reading the status
		// back from the database, which may be unreachable if the load failed because of an outage
		boolean success = false;
		try {
			String dataTable = getDataTable();
			if (dataTable == null) {
				log.error("Dataset has no data table assigned");
				return false;
			}

			markAsProcessing();

			// A replace loads into a shadow table, which replaces the data table at the end of the load in the same
			// transaction. Until then, and if the load fails, queries and exports keep serving the previous data.
			String targetTable = append ? dataTable : PgDatasets.shadowTableName(dataTable);
			columnSize.clear();

			Date before = new Date();
			Connection conn = null;
			PreparedStatement stmt = null;
			CSVReader cr = null;
			try {
				conn = rowstore.getConnection();
				char separator = detectSeparator(csvFile);

				ICSVParser csvParser;
				if (rowstore.getConfig().isLegacyParserEnabled()) {
					csvParser = new CSVParserBuilder().
							withSeparator(separator).
							withQuoteChar('"').
							build();
				} else {
					csvParser = new RFC4180ParserBuilder().
							withSeparator(separator).
							withQuoteChar('"').
							build();
				}
				LineTrackingReader lineReader = new LineTrackingReader(Files.newBufferedReader(csvFile.toPath(), DatasetUtil.detectCharset(csvFile)));
				cr = new CSVReaderBuilder(lineReader).
						withSkipLines(0).
						withCSVParser(csvParser).
						build();
				long lineCount = 0;
				Set<String> labels = new LinkedHashSet<>();
				String[] line;

				conn.setAutoCommit(false);

				if (!append) {
					createShadowTable(conn, targetTable);
				}

				stmt = conn.prepareStatement("INSERT INTO " + targetTable + " (data) VALUES (?)");
				long emptyLineNumber = 0;
				while (true) {
					long linesBefore = cr.getLinesRead();
					line = cr.readNext();
					long linesRead = cr.getLinesRead();
					if (linesRead == linesBefore) {
						// end of file
						break;
					}
					// An empty line (outside a quoted value) is not a record. The parsers do not report it reliably:
					// RFC4180Parser returns null, which is not the end of the file, and CSVParser returns the same
					// record as for a quoted empty value. Therefore the line itself is checked.
					if (line == null || (linesRead == linesBefore + 1 && lineReader.getLastLine().isEmpty())) {
						if (emptyLineNumber == 0) {
							emptyLineNumber = linesRead;
						}
						continue;
					}
					// empty lines are only allowed at the end of the file
					if (emptyLineNumber > 0) {
						log.error("Line {} of CSV is empty, but it is followed by further rows", emptyLineNumber);
						log.info("Rolling back transaction");
						conn.rollback();
						return false;
					}
					if (lineCount == 0) {
						// We convert all column names to lower case,
						// otherwise all queries must be case sensitive later
						for (String s : line) {
							String l = s.trim().toLowerCase();
							if (l.length() > 0) {
								labels.add(l);
							} else {
								log.debug("Skipping column due to empty label");
							}
						}

						if (append) {
							// we must compare existing column names with new ones
							Set<String> oldColumnNames = getColumnNames(false);

							// if there are no old column names we assume this dataset is newly created
							if (oldColumnNames.size() > 0 &&
									!((oldColumnNames.size() == labels.size()) && oldColumnNames.containsAll(labels))) {
								log.error("Column name mismatch: new tabular structure does not equal existing structure");
								log.error("Rolling back transaction");
								conn.rollback();
								return false;
							}
						}
					} else {
						JSONObject jsonLine = null;
						try {
							jsonLine = csvLineToJsonObject(line, labels);
						} catch (Exception e) {
							log.error(e.getMessage());
							log.error("Error occured when processing line {} of CSV: {}", lineCount, line);
							log.info("Rolling back transaction");
							conn.rollback();
							return false;
						}
						PGobject jsonb = new PGobject();
						jsonb.setType("jsonb");
						jsonb.setValue(jsonLine.toString());
						stmt.setObject(1, jsonb);
						log.debug("Adding to batch: " + stmt);
						stmt.addBatch();
						// we execute the batch every 100th line
						if ((lineCount % 200) == 0) {
							log.debug("Executing: " + stmt);
							stmt.executeBatch();
						}
					}
					lineCount++;
				}
				// in case there are some inserts left to be sent (i.e.
				// batch size above was smaller than 100 when loop ended)
				log.debug("Executing: " + stmt);
				stmt.executeBatch();

				if (append) {
					createIndexes(conn, targetTable, labels);
				} else if (!replaceDataTable(conn, targetTable, createIndexes(conn, targetTable, labels))) {
					log.info("Rolling back transaction");
					conn.rollback();
					return false;
				}

				// we commit the transaction and free the resources of the statement
				conn.commit();
				success = true;
			} catch (SQLException e) {
				SqlExceptionLogUtil.error(log, e);
				try {
					log.info("Rolling back transaction");
                    if (conn != null) {
                        conn.rollback();
                    }
                } catch (SQLException e1) {
					SqlExceptionLogUtil.error(log, e1);
				}
				return false;
			} catch (CsvValidationException e) {
				log.error(e.getMessage());
				try {
					log.info("Rolling back transaction");
					conn.rollback();
				} catch (SQLException e1) {
					SqlExceptionLogUtil.error(log, e1);
				}
				return false;
			} finally {
				if (cr != null) {
					try {
						cr.close();
					} catch (IOException e) {
						log.error(e.getMessage());
					}
				}
				DatasetUtil.closeStatement(stmt);
				if (conn != null) {
					try {
						conn.close();
					} catch (SQLException e) {
						SqlExceptionLogUtil.error(log, e);
					}
				}

				log.debug("Populating dataset took " + (new Date().getTime() - before.getTime()) + " ms");
			}
		} finally {
			// Also reached by exceptions, which propagate to the caller after the status has been set
			if (!writeStatusWithRetry(success ? EtlStatus.AVAILABLE : EtlStatus.ERROR)) {
				log.error("Dataset {} keeps its last known status {}({}) until it is loaded again{}", getId(),
						EtlStatus.toString(status), status, status == EtlStatus.PROCESSING ? " or RowStore is restarted" : "");
			}
		}

		return success;
	}

	/**
	 * Creates an index for each field that has no index yet and whose values are not too large.
	 *
	 * @return Returns the names of the created indexes.
	 */
	private List<String> createIndexes(Connection conn, String table, Set<String> fields) throws SQLException {
		long before = System.currentTimeMillis();
		List<String> createdIndexes = new ArrayList<>();
		Set<String> existingIndices = getIndexNames(conn, table);
		for (String field : fields) {
			// We do not try to index fields that are too large as we would get an error from PostgreSQL
			// TODO instead of just skipping the index we could run a fulltext-index on such fields
			Integer fieldSize = columnSize.get(field);
			if (fieldSize != null && fieldSize > maxSizeForIndex) {
				log.warn("Skipping index creation for field \"" + field + "\"; the configured max field size is " + maxSizeForIndex + ", but the actual size is " + fieldSize);
				continue;
			}
			String indexName = table + "_jsonidx_" + Hashing.md5(field).substring(0, 8);
			if (existingIndices.contains(indexName)) {
				log.debug("Index with name " + indexName + " already exists, skipping creation");
				continue;
			}
			// We cannot use prepared statements for CREATE INDEX with parametrized fields:
			// the type to be used with setObject() is not known and setString() does not work.
			// It should be safe to run BaseConnection.escapeString() to avoid SQL-injection
			String sql = new StringBuilder("CREATE INDEX ").
					append(indexName).
					append(" ON ").
					append(table).
					append(" ((data->>'").
					append(conn.unwrap(BaseConnection.class).escapeString(field)).
					append("') text_pattern_ops)").
					toString();
			log.debug("Executing: " + sql);
			try (Statement stmt = conn.createStatement()) { stmt.execute(sql); }
			createdIndexes.add(indexName);
		}
		/*
		The index below may be used for more advanced JSON-specific indexing
		and querying, but currently we don't need this functionality

		String sql = new StringBuilder("CREATE INDEX ON ").append(table).append(" USING GIN(data jsonb_path_ops)").toString();
		log.debug("Executing: " + sql);
		conn.createStatement().execute(sql);
		*/

		log.debug("Creating indexes took {} ms", System.currentTimeMillis() - before);
		return createdIndexes;
	}

	/**
	 * Creates the shadow table for a replace, within the load's transaction. A shadow table is never committed under
	 * its own name, but a table left behind under this name must not be loaded into, so it is dropped first.
	 */
	private void createShadowTable(Connection conn, String shadowTable) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			executeDdl(stmt, "DROP TABLE IF EXISTS " + shadowTable);
		}
		PgDatasets.createDataTable(conn, shadowTable);
	}

	/**
	 * Replaces the data table with the shadow table, within the load's transaction: drops the data table (with its
	 * indexes and sequence) and renames the shadow table, its indexes, primary key and sequence to the names they
	 * would have if they had been created for the data table, so that the next replace does not collide with them.
	 *
	 * @param shadowIndexes The names of the indexes created for the shadow table.
	 * @return Returns false if interrupted while waiting to lock the data table, see {@link #lockDataTableForReplace(Connection)}.
	 */
	private boolean replaceDataTable(Connection conn, String shadowTable, List<String> shadowIndexes) throws SQLException {
		long before = System.currentTimeMillis();
		try {
			if (!lockDataTableForReplace(conn)) {
				return false;
			}
			try (Statement stmt = conn.createStatement()) {
				copyOwnerAndPrivileges(conn, stmt, shadowTable);
				executeDdl(stmt, "DROP TABLE " + dataTable);
				executeDdl(stmt, "ALTER TABLE " + shadowTable + " RENAME TO " + dataTable);
				for (String index : shadowIndexes) {
					executeDdl(stmt, "ALTER INDEX " + index + " RENAME TO " + dataTable + index.substring(shadowTable.length()));
				}
				// PostgreSQL chooses the names of the primary key and the sequence, which may have a numeric suffix
				String primaryKey = queryForIdentifier(conn, "SELECT quote_ident(conname) FROM pg_constraint " +
						"WHERE conrelid = '" + dataTable + "'::regclass AND contype = 'p'");
				executeDdl(stmt, "ALTER TABLE " + dataTable + " RENAME CONSTRAINT " + primaryKey + " TO " + dataTable + "_pkey");
				String sequence = queryForIdentifier(conn, "SELECT pg_get_serial_sequence('" + dataTable + "', 'rownr')");
				executeDdl(stmt, "ALTER SEQUENCE " + sequence + " RENAME TO " + dataTable + "_rownr_seq");
			}
			return true;
		} finally {
			log.debug("Replacing data table took {} ms", System.currentTimeMillis() - before);
		}
	}

	/**
	 * Gives the shadow table the owner and the table privileges of the data table, which would otherwise be lost when
	 * the shadow table replaces the data table (e.g., SELECT granted to a separate user of a read replica).
	 */
	private void copyOwnerAndPrivileges(Connection conn, Statement stmt, String shadowTable) throws SQLException {
		List<String> owner = queryForStrings(conn, "SELECT quote_ident(pg_get_userbyid(d.relowner)) " +
				"FROM pg_class d, pg_class s WHERE d.oid = '" + dataTable + "'::regclass " +
				"AND s.oid = '" + shadowTable + "'::regclass AND d.relowner <> s.relowner");
		for (String o : owner) {
			executeDdl(stmt, "ALTER TABLE " + shadowTable + " OWNER TO " + o);
		}
		// the owner's own privileges are implicit
		List<String> grants = queryForStrings(conn, "SELECT 'GRANT ' || a.privilege_type || ' ON " + shadowTable + " TO ' || " +
				"CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END || " +
				"CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END " +
				"FROM pg_class d CROSS JOIN LATERAL aclexplode(d.relacl) a " +
				"WHERE d.oid = '" + dataTable + "'::regclass AND a.grantee <> d.relowner");
		for (String grant : grants) {
			executeDdl(stmt, grant);
		}
	}

	/**
	 * Locks the data table exclusively for replacing it, within the load's transaction. The lock has to wait for
	 * readers of the data table, e.g., a long streaming export, and while it waits, it blocks new queries of the
	 * data table. Therefore the first attempts wait at most {@link #SWAP_LOCK_TIMEOUT} each and are retried after a
	 * delay, so that queries are not blocked for longer than that at a time. The last attempt waits without a
	 * timeout, so that a replace eventually succeeds even if the data table is read for a long time (e.g., by a
	 * backup); meanwhile new queries of the data table wait as well.
	 *
	 * After the lock has been acquired, the attempt's lock_timeout stays in force until the end of the transaction.
	 * This is harmless, since the remaining statements only touch the locked data table, objects that depend on it,
	 * and the shadow table, which was created in the same transaction.
	 *
	 * @return Returns true if the lock was acquired, false if interrupted while waiting for the next attempt.
	 * @throws SQLException If locking failed for another reason than a lock timeout of an attempt with a timeout.
	 */
	boolean lockDataTableForReplace(Connection conn) throws SQLException {
		for (int attempt = 0; ; attempt++) {
			boolean lastAttempt = attempt >= swapRetryDelaysMillis.length;
			Savepoint savepoint = conn.setSavepoint();
			try (Statement stmt = conn.createStatement()) {
				executeDdl(stmt, "SET LOCAL lock_timeout = '" + (lastAttempt ? "0" : SWAP_LOCK_TIMEOUT) + "'");
				executeDdl(stmt, "LOCK TABLE " + dataTable + " IN ACCESS EXCLUSIVE MODE");
				return true;
			} catch (SQLException e) {
				if (lastAttempt || !LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
					throw e;
				}
				// also reverts SET LOCAL
				conn.rollback(savepoint);
			}
			long delay = swapRetryDelaysMillis[attempt];
			log.warn("Data table {} of dataset {} is in use (e.g., by an export), retrying to replace it in {} ms{}",
					dataTable, getId(), delay, attempt == swapRetryDelaysMillis.length - 1 ? " without a lock timeout" : "");
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				log.error("Interrupted while waiting to replace data table {} of dataset {}", dataTable, getId());
				return false;
			}
		}
	}

	/**
	 * Sets the delays between the attempts to lock the data table for a replace. Intended for tests.
	 */
	void setSwapRetryDelays(long... delaysMillis) {
		this.swapRetryDelaysMillis = delaysMillis.clone();
	}

	private static void executeDdl(Statement stmt, String sql) throws SQLException {
		log.debug("Executing: " + sql);
		stmt.execute(sql);
	}

	private static List<String> queryForStrings(Connection conn, String sql) throws SQLException {
		List<String> result = new ArrayList<>();
		try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
			while (rs.next()) {
				result.add(rs.getString(1));
			}
		}
		return result;
	}

	private static String queryForIdentifier(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
			if (!rs.next() || rs.getString(1) == null) {
				throw new SQLException("No result for: " + sql);
			}
			return rs.getString(1);
		}
	}

	private Set<String> getIndexNames(Connection conn, String table) throws SQLException {
		long before = System.currentTimeMillis();
		String sql = "SELECT ci.relname AS indexname " +
				"FROM pg_index i, pg_class ci, pg_class ct " +
				"WHERE i.indexrelid = ci.oid AND " +
				"i.indrelid = ct.oid AND " +
				"ct.relname = ? AND " +
				"ci.relname LIKE '%_jsonidx_%'";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, table);
			Set<String> result = new HashSet<>();
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					result.add(rs.getString(1));
				}
			}
			return result;
		} finally {
			log.debug("Fetching index names took {} ms", System.currentTimeMillis() - before);
		}
	}

	/**
	 * @see Dataset#query(Map, int, int)
	 */
	@Override
	public QueryResult query(Map<String, String> tuples, int limit, int offset) {
		long totalTime = System.currentTimeMillis();
		long queryTime = -1;

		List<JSONObject> result = new ArrayList<>();
		long resultCount = 0;
		int regexp = rowstore.getConfig().getRegexpQuerySupport();
		boolean optimizeRegexp = true;

		// Build query template first (no DB resources needed yet)
		StringBuilder queryTemplate = new StringBuilder("SELECT data, count(*) OVER() AS result_count FROM " + getDataTable());
		if (!tuples.isEmpty()) {
			String[] values = tuples.values().toArray(new String[tuples.size()]);
			for (int i = 0; i < tuples.size(); i++) {
				if (values[i].equals("~")) {
					log.debug("No value provided after ~");
					return new QueryResult.Error("400");
				}

				if (i == 0) {
					queryTemplate.append(" WHERE ");
				} else {
					queryTemplate.append(" AND ");
				}

				if (regexp == Dataset.REGEXP_QUERY_FULL && values[i].startsWith("~")) {
					optimizeRegexp = false;
				}

				boolean useRegex = false;
				if (regexp == Dataset.REGEXP_QUERY_FULL && (!optimizeRegexp || DatasetUtil.isRegExpString(values[i]))) {
					useRegex = true;
				} else if (regexp == Dataset.REGEXP_QUERY_SIMPLE && values[i].startsWith("^")) {
					useRegex = true;
				}

				if (useRegex) {
					String regexValue = values[i].startsWith("~") ? values[i].substring(1) : values[i];
					if (!DatasetUtil.isSafeRegex(regexValue)) {
						log.debug("Rejected unsafe regex pattern: length={}", regexValue.length());
						return new QueryResult.Error("400");
					}
					queryTemplate.append("data->>? ~ ?");
				} else {
					queryTemplate.append("data->>? = ?");
				}
			}
		}
		queryTemplate.append(" ORDER BY rownr LIMIT ? OFFSET ? ");

		try (Connection c = rowstore.getQueryConnection();
			 PreparedStatement s = c.prepareStatement(queryTemplate.toString())) {

			int paramPos = 1;
			if (!tuples.isEmpty()) {
				for (String key : tuples.keySet()) {
					s.setString(paramPos, key.toLowerCase());
					String value = tuples.get(key);
					if (!optimizeRegexp && value.startsWith("~")) {
						value = value.substring(1);
					}
					s.setString(paramPos + 1, value);
					paramPos += 2;
				}
			}

			s.setInt(paramPos++, limit);
			s.setInt(paramPos, offset);

			log.debug("Executing: " + s);

			int queryTO = rowstore.getConfig().getQueryTimeout();
			if (queryTO > -1) {
				s.setQueryTimeout(queryTO);
			}
			queryTime = System.currentTimeMillis();
			try (ResultSet r = s.executeQuery()) {
				queryTime = System.currentTimeMillis() - queryTime;
				while (r.next()) {
					String value = r.getString("data");
					if (resultCount == 0) {
						resultCount = r.getLong("result_count");
					}
					try {
						result.add(new JSONObject(value));
					} catch (JSONException e) {
						log.error(e.getMessage());
					}
				}
			}
		} catch (SQLException e) {
			log.debug(e.getMessage());
			return new QueryResult.Error(e.getSQLState());
		} finally {
			log.debug("Performing database query took {} ms, total time was {} ms", queryTime, System.currentTimeMillis() - totalTime);
		}

		return new QueryResult(result, limit, offset, resultCount, queryTime);
	}

	public ResultSet streamAll() {
		Connection conn = null;
		try {
			conn = rowstore.getQueryConnection();
			conn.setAutoCommit(false);
			PreparedStatement stmnt = conn.prepareStatement("SELECT data FROM " + getDataTable(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			stmnt.setFetchSize(1000);
			return stmnt.executeQuery();
		} catch (SQLException e) {
			log.warn(e.getMessage());
			return null;
		}
	}

	/**
	 * @see Dataset#getColumnNames()
	 */
	public Set<String> getColumnNames() {
		return this.getColumnNames(true);
	}

	private Set<String> getColumnNames(boolean useQueryDatabase) {
		long before = System.currentTimeMillis();
		JdbcTemplate jt = useQueryDatabase ? rowstore.getQueryJdbcTemplate() : rowstore.getJdbcTemplate();
		String sql = "SELECT DISTINCT jsonb_object_keys(data) AS column_names FROM " + getDataTable() +
				" WHERE rownr=(SELECT min(rownr) FROM " + getDataTable() + ")";
		try {
			return new HashSet<>(jt.queryForList(sql, String.class));
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			return new HashSet<>();
		} finally {
			log.debug("Fetching column names took {} ms", System.currentTimeMillis() - before);
		}
	}

	/**
	 * Initializes the object by loading all information from the database.
	 *
	 * @throws IllegalStateException If there is no dataset with this ID.
	 * @throws IllegalArgumentException If the ID is not a valid UUID.
	 * @throws DataAccessException If the database could not be queried, e.g., because it is unreachable.
	 */
	private void initFromDb() {
		long before = System.currentTimeMillis();
		try {
			log.debug("Loading dataset {} from database", getId());
			Map<String, Object> row = rowstore.getJdbcTemplate().queryForMap(
					"SELECT * FROM " + PgDatasets.DATASETS_TABLE_NAME + " WHERE id = ?::uuid", getId());
			this.canonicalId = row.get("id").toString();
			this.status = (int) row.get("status");
			this.created = (java.sql.Timestamp) row.get("created");
			String loadedTable = (String) row.get("data_table");
			if (loadedTable != null) {
				loadedTable = loadedTable.trim();
				validateDataTableName(loadedTable);
			}
			this.dataTable = loadedTable;
		} catch (EmptyResultDataAccessException e) {
			throw new IllegalStateException("Unable to initialize Dataset object from database");
		} catch (DataAccessException e) {
			Throwable cause = e.getCause();
			if (cause instanceof SQLException se && "22P02".equalsIgnoreCase(se.getSQLState())) {
				// invalid UUID input, treated like an unknown dataset
				log.info(se.getMessage().replaceFirst("ERROR: ", ""));
				throw new IllegalArgumentException(e);
			}
			// any other failure must not be mistaken for an unknown dataset; connectivity failures
			// (DataAccessResourceFailureException) are mapped to 503 by DatabaseExceptionHandler
			if (cause instanceof SQLException se) {
				SqlExceptionLogUtil.error(log, se);
			} else {
				log.error(e.getMessage());
			}
			throw e;
		} finally {
			log.debug("Loading dataset took {} ms", System.currentTimeMillis() - before);
		}
	}

	/**
	 * @see Dataset#getRowCount()
	 */
	@Override
	public long getRowCount() {
		long before = System.currentTimeMillis();
		try {
			Long result = rowstore.getQueryJdbcTemplate().queryForObject(
					"SELECT COUNT(rownr)::BIGINT FROM " + getDataTable(), Long.class);
			return result != null ? result : -1;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			return -1;
		} finally {
			log.debug("Fetching row count took {} ms", System.currentTimeMillis() - before);
		}
	}

	/**
	 * @see Dataset#getAliases()
	 */
	@Override
	public Set<String> getAliases() {
		return this.getAliases(true);
	}

	private Set<String> getAliases(boolean useQueryDatabase) {
		long before = System.currentTimeMillis();
		JdbcTemplate jt = useQueryDatabase ? rowstore.getQueryJdbcTemplate() : rowstore.getJdbcTemplate();
		try {
			return new HashSet<>(jt.queryForList(
					"SELECT alias FROM " + PgDatasets.ALIAS_TABLE_NAME + " WHERE dataset_id = ?::uuid",
					String.class, getId()));
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			return new HashSet<>();
		} finally {
			log.debug("Fetching aliases took {} ms", System.currentTimeMillis() - before);
		}
	}

	/**
	 * @see Dataset#setAliases(Set)
	 */
	@Override
	public boolean setAliases(Set<String> aliases) {
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		long before = System.currentTimeMillis();
		try (Connection conn = rowstore.getConnection()) {
			conn.setAutoCommit(false);

			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(id);

			// Acquire row-level lock to serialize concurrent alias modifications
			try (PreparedStatement lockStmt = conn.prepareStatement("SELECT 1 FROM " + PgDatasets.DATASETS_TABLE_NAME + " WHERE id = ? FOR UPDATE")) {
				lockStmt.setObject(1, uuid);
				lockStmt.executeQuery();
			}

			Set<String> existingAliases = getAliases(false);

			try (PreparedStatement deleteStmt = conn.prepareStatement("DELETE FROM " + PgDatasets.ALIAS_TABLE_NAME + " WHERE dataset_id = ?")) {
				deleteStmt.setObject(1, uuid);
				log.debug("Executing: " + deleteStmt);
				deleteStmt.execute();
			}

			try (PreparedStatement insertStmt = conn.prepareStatement("INSERT INTO " + PgDatasets.ALIAS_TABLE_NAME + " (dataset_id, alias) VALUES (?, ?)")) {
				for (String alias : aliases) {
					if (existingAliases.contains(alias) || (isAliasValid(alias) && isAliasAvailable(alias))) {
						insertStmt.setObject(1, uuid);
						insertStmt.setString(2, alias);
						log.debug("Adding to batch: " + insertStmt);
						insertStmt.addBatch();
					} else {
						log.debug("Received invalid or unavailable alias, rolling back");
						conn.rollback();
						return false;
					}
				}
				log.debug("Executing and committing batch");
				insertStmt.executeBatch();
			}
			conn.commit();
		} catch (SQLException e) {
			log.error(e.getMessage());
			return false;
		} finally {
			log.debug("Setting aliases took {} ms", System.currentTimeMillis() - before);
		}

		return true;
	}

	public String resolveAlias(String alias) {
		long before = System.currentTimeMillis();
		try {
			List<UUID> results = rowstore.getJdbcTemplate().query(
					"SELECT dataset_id FROM " + PgDatasets.ALIAS_TABLE_NAME + " WHERE alias = ?",
					(rs, rowNum) -> (UUID) rs.getObject("dataset_id"), alias);
			return results.isEmpty() ? null : results.get(0).toString();
		} catch (DataAccessResourceFailureException e) {
			// An unreachable database must not be mistaken for an unknown alias (mapped to 503). Unlike in
			// initFromDb(), other failures are still treated as an unknown alias because the alias is
			// unvalidated user input, which may be rejected by the database (e.g., invalid byte sequences).
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			return null;
		} finally {
			log.debug("Resolving alias took {} ms", System.currentTimeMillis() - before);
		}
	}

	private boolean isAliasValid(String alias) {
		return alias != null && StringUtils.isAlphanumeric(alias);
	}

	private boolean isAliasAvailable(String alias) {
		return (!rowstore.getDatasets().hasDataset(alias) && (resolveAlias(alias) == null));
	}

	/**
	 * Converts a CSV row to a JSON object.
	 *
	 * @param line The row consisting of its cells' values.
	 * @param labels The column labels.
	 * @return Returns a JSON object consisting of key (labels) - value (line/cell values) pairs.
	 * @throws JSONException
	 */
	private JSONObject csvLineToJsonObject(String[] line, Set<String> labels) throws JSONException {
		if (line.length > labels.size()) {
			throw new IllegalArgumentException("Amount of values per row must not be higher than amount of labels in first row of CSV file");
		}

		JSONObject result = new JSONObject();
		String[] labelArr = labels.toArray(new String[0]);
		for (int i = 0; i < line.length; i++) {
			// we skip empty strings as this would result in empty key names in the JSON result
			if (labelArr[i].trim().isEmpty()) {
				continue;
			}
			result.put(labelArr[i], line[i]);
			putAndRetainLargestValue(labelArr[i], line[i].length());
		}

		return result;
	}

	private void putAndRetainLargestValue(String key, int length) {
		Integer existing = columnSize.get(key);
		if (existing == null || (existing < length)) {
			columnSize.put(key, length);
		}
	}

	private char detectSeparator(File csvFile) {
		char result = ',';
		BufferedReader br = null;
		try {
			br = Files.newBufferedReader(csvFile.toPath(), DatasetUtil.detectCharset(csvFile));
			String line1 = br.readLine();
			String line2 = br.readLine();
			int semiCount1 = StringUtils.countMatches(line1, ";");
			if ((semiCount1 > 0) && (semiCount1 == StringUtils.countMatches(line2, ";"))) {
				result = ';';
				log.debug("Detected use of semicolon as CSV separator");
			} else {
				log.debug("No semicolon detected, defaulting to comma as CSV separator");
			}
		} catch (IOException e) {
			log.info(e.getMessage());
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}
		return result;
	}

	/**
	 * Remembers the last line that was read, so that empty lines can be detected.
	 */
	private static final class LineTrackingReader extends BufferedReader {

		private String lastLine;

		LineTrackingReader(Reader in) {
			super(in);
		}

		@Override
		public String readLine() throws IOException {
			lastLine = super.readLine();
			return lastLine;
		}

		String getLastLine() {
			return lastLine;
		}

	}

}
