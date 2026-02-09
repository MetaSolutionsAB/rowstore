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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

	private int status;

	private Date created;

	private String dataTable;

	private final RowStore rowstore;

	private final Map<String, Integer> columnSize = new HashMap<>();

	private final int maxSizeForIndex = 256;

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
		long before = System.currentTimeMillis();
		try (Connection conn = rowstore.getConnection();
			 PreparedStatement stmt = conn.prepareStatement("UPDATE " + PgDatasets.DATASETS_TABLE_NAME + " SET status = ? WHERE id = ?")) {
			conn.setAutoCommit(true);
			stmt.setInt(1, status);
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(id);
			stmt.setObject(2, uuid);
			log.info("Setting status of " + getId() + " to " + EtlStatus.toString(status) + "(" + status + ")");
			log.debug("Executing: " + stmt);
			stmt.executeUpdate();
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			log.debug("Setting status took {} ms", System.currentTimeMillis() - before);
		}

		this.status = status;
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

		String dataTable = getDataTable();
		if (dataTable == null) {
			log.error("Dataset has no data table assigned");
			return false;
		}

		try {
			setStatus(EtlStatus.PROCESSING);

			if (!append) {
				if (!truncateTable()) {
					setStatus(EtlStatus.ERROR);
					return false;
				}
			}

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
				cr = new CSVReaderBuilder(Files.newBufferedReader(csvFile.toPath(), DatasetUtil.detectCharset(csvFile))).
						withSkipLines(0).
						withCSVParser(csvParser).
						build();
				long lineCount = 0;
				Set<String> labels = new LinkedHashSet<>();
				String[] line;

				conn.setAutoCommit(false);

				stmt = conn.prepareStatement("INSERT INTO " + dataTable + " (data) VALUES (?)");
				while ((line = cr.readNext()) != null) {
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
							setStatus(EtlStatus.ERROR);
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

				createIndexes(conn, labels);

				// we commit the transaction and free the resources of the statement
				conn.commit();

				setStatus(EtlStatus.AVAILABLE);
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
				setStatus(EtlStatus.ERROR);
				return false;
			} catch (CsvValidationException e) {
				log.error(e.getMessage());
				try {
					log.info("Rolling back transaction");
					conn.rollback();
				} catch (SQLException e1) {
					SqlExceptionLogUtil.error(log, e1);
				}
				setStatus(EtlStatus.ERROR);
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
			if (getStatus() == EtlStatus.PROCESSING) {
				// something has gone wrong if the status is still "processing"
				// at this point, so we set it to "error", just in case
				setStatus(EtlStatus.ERROR);
				return false;
			}
		}

		return true;
	}

	private void createIndexes(Connection conn, Set<String> fields) throws SQLException {
		long before = System.currentTimeMillis();
		Set<String> existingIndices = getIndexNames();
		for (String field : fields) {
			// We do not try to index fields that are too large as we would get an error from PostgreSQL
			// TODO instead of just skipping the index we could run a fulltext-index on such fields
			Integer fieldSize = columnSize.get(field);
			if (fieldSize != null && fieldSize > maxSizeForIndex) {
				log.warn("Skipping index creation for field \"" + field + "\"; the configured max field size is " + maxSizeForIndex + ", but the actual size is " + fieldSize);
				continue;
			}
			String indexName = dataTable + "_jsonidx_" + Hashing.md5(field).substring(0, 8);
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
					append(dataTable).
					append(" ((data->>'").
					append(conn.unwrap(BaseConnection.class).escapeString(field)).
					append("') text_pattern_ops)").
					toString();
			log.debug("Executing: " + sql);
			try (Statement stmt = conn.createStatement()) { stmt.execute(sql); }
		}
		/*
		The index below may be used for more advanced JSON-specific indexing
		and querying, but currently we don't need this functionality

		String sql = new StringBuilder("CREATE INDEX ON ").append(table).append(" USING GIN(data jsonb_path_ops)").toString();
		log.debug("Executing: " + sql);
		conn.createStatement().execute(sql);
		*/

		log.debug("Creating indexes took {} ms", System.currentTimeMillis() - before);
	}

	private boolean truncateTable() {
		long before = System.currentTimeMillis();
		try (Connection conn = rowstore.getConnection();
			 Statement stmt = conn.createStatement()) {
			conn.setAutoCommit(false);

			log.debug("Truncating contents of table " + dataTable);
			String truncTable = "TRUNCATE " + dataTable;
			stmt.executeUpdate(truncTable);
			log.debug("Executing: " + truncTable);

			log.debug("Removing all indexes from table " + dataTable);
			Set<String> existingIndices = getIndexNames();
			for (String index : existingIndices) {
				String sql = "DROP INDEX IF EXISTS " + index;
				log.debug("Executing: " + sql);
				stmt.executeUpdate(sql);
			}

			conn.commit();
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
			return false;
		} finally {
			log.debug("Truncating table took {} ms", System.currentTimeMillis() - before);
		}

		return true;
	}

	private Set<String> getIndexNames() {
		long before = System.currentTimeMillis();
		Set<String> result = new HashSet<>();
		String sql = "SELECT ci.relname AS indexname " +
				"FROM pg_index i,pg_class ci,pg_class ct " +
				"WHERE i.indexrelid=ci.oid AND " +
				"i.indrelid=ct.oid AND " +
				"ct.relname='" + dataTable + "' AND " +
				"ci.relname LIKE '%_jsonidx_%'";
		try (Connection conn = rowstore.getConnection();
			 Statement stmnt = conn.createStatement();
			 ResultSet rs = stmnt.executeQuery(sql)) {
			log.debug("Executing: " + sql);
			while (rs.next()) {
				result.add(rs.getString("indexname"));
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			log.debug("Fetching index names took {} ms", System.currentTimeMillis() - before);
		}

		return result;
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
		Set<String> result = new HashSet<>();
		String sql = "SELECT DISTINCT jsonb_object_keys(data) AS column_names FROM " + getDataTable() +
				" WHERE rownr=(SELECT min(rownr) FROM " + getDataTable() + ")";
		try (Connection conn = useQueryDatabase ? rowstore.getQueryConnection() : rowstore.getConnection();
			 PreparedStatement stmt = conn.prepareStatement(sql);
			 ResultSet rs = stmt.executeQuery()) {
			log.debug("Executing: " + stmt);
			while (rs.next()) {
				result.add(rs.getString("column_names"));
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			log.debug("Fetching column names took {} ms", System.currentTimeMillis() - before);
		}

		return result;
	}

	/**
	 * Initializes the object by loading all information from the database.
	 */
	private void initFromDb() {
		long before = System.currentTimeMillis();
		try (Connection conn = rowstore.getConnection();
			 PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + PgDatasets.DATASETS_TABLE_NAME + " WHERE id = ?")) {
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(getId());
			stmt.setObject(1, uuid);
			log.debug("Loading dataset " + getId() + " from database");
			log.debug("Executing: " + stmt);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					this.status = rs.getInt("status");
					this.created = rs.getTimestamp("created");
					String loadedTable = rs.getString("data_table");
					if (loadedTable != null) {
						loadedTable = loadedTable.trim();
						validateDataTableName(loadedTable);
					}
					this.dataTable = loadedTable;
				} else {
					throw new IllegalStateException("Unable to initialize Dataset object from database");
				}
			}
		} catch (SQLException e) {
			if (!"22P02".equalsIgnoreCase(e.getSQLState())) {
				SqlExceptionLogUtil.error(log, e);
			} else {
				log.info(e.getMessage().replaceFirst("ERROR: ", ""));
			}
			throw new IllegalArgumentException(e);
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
		long result = -1;
		String sql = "SELECT COUNT(rownr)::BIGINT AS rowcount FROM " + getDataTable();
		try (Connection conn = rowstore.getQueryConnection();
			 PreparedStatement stmt = conn.prepareStatement(sql);
			 ResultSet rs = stmt.executeQuery()) {
			log.debug("Executing: " + stmt);
			if (rs.next()) {
				result = rs.getLong("rowcount");
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			log.debug("Fetching row count took {} ms", System.currentTimeMillis() - before);
		}

		return result;
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
		Set<String> result = new HashSet<>();
		try (Connection conn = useQueryDatabase ? rowstore.getQueryConnection() : rowstore.getConnection();
			 PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + PgDatasets.ALIAS_TABLE_NAME + " WHERE dataset_id = ?")) {
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(getId());
			stmt.setObject(1, uuid);
			log.debug("Loading aliases for dataset " + getId());
			log.debug("Executing: " + stmt);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					result.add(rs.getString("alias"));
				}
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			log.debug("Fetching aliases took {} ms", System.currentTimeMillis() - before);
		}

		return result;
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
		try (Connection conn = rowstore.getConnection();
			 PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + PgDatasets.ALIAS_TABLE_NAME + " WHERE alias = ?")) {
			stmt.setString(1, alias);
			log.debug("Executing: " + stmt);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					UUID uuid = (UUID) rs.getObject("dataset_id");
					return uuid.toString();
				}
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			log.debug("Resolving alias took {} ms", System.currentTimeMillis() - before);
		}

		return null;
	}

	private boolean isAliasValid(String alias) {
		if (alias == null) {
			return false;
		}

		if (!StringUtils.isAlphanumeric(alias)) {
			return false;
		}

		return true;
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

}