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

import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.detect.AutoDetectReader;
import org.apache.tika.exception.TikaException;
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
import org.restlet.data.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

	private static Logger log = LoggerFactory.getLogger(PgDataset.class);

	private String id;

	private int status;

	private Date created;

	private String dataTable;

	private RowStore rowstore;

	private Map<String, Integer> columnSize = new HashMap<>();

	private int maxSizeForIndex = 256;

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
		} else if (this.id.length() < 36) {
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
		Date before = new Date();
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = rowstore.getConnection();
			conn.setAutoCommit(true);
			stmt = conn.prepareStatement("UPDATE " + PgDatasets.DATASETS_TABLE_NAME + " SET status = ? WHERE id = ?");
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
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}

			log.debug("Setting status took " + (new Date().getTime() - before.getTime()) + " ms");
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
				cr = new CSVReader(new AutoDetectReader(new FileInputStream(csvFile)), separator, '"');
				long lineCount = 0;
				String[] labels = null;
				String[] line;

				conn.setAutoCommit(false);

				stmt = conn.prepareStatement("INSERT INTO " + dataTable + " (data) VALUES (?)");
				while ((line = cr.readNext()) != null) {
					if (lineCount == 0) {
						labels = new String[line.length];
						// We convert all column names to lower case,
						// otherwise all queries must be case sensitive later
						for (int i = 0; i < line.length; i++) {
							labels[i] = line[i].trim().toLowerCase();
						}

						if (append) {
							// we must compare existing column names with new ones
							Set<String> newColumnNames = new HashSet<String>(Arrays.asList(labels));
							Set<String> oldColumnNames = getColumnNames(false);

							// if there are no old column names we assume this dataset is newly created
							if (oldColumnNames.size() > 0 &&
									!((oldColumnNames.size() == newColumnNames.size()) && oldColumnNames.containsAll(newColumnNames))) {
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

				createIndexes(conn, new HashSet<String>(Arrays.asList(labels)));

				// we commit the transaction and free the resources of the statement
				conn.commit();

				setStatus(EtlStatus.AVAILABLE);
			} catch (TikaException te) {
				log.error(te.getMessage());
				setStatus(EtlStatus.ERROR);
				return false;
			} catch (SQLException e) {
				SqlExceptionLogUtil.error(log, e);
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
				if (stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						SqlExceptionLogUtil.error(log, e);
					}
				}
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
		Date before = new Date();
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
					append(((BaseConnection) conn).escapeString(field)).
					append("') text_pattern_ops)").
					toString();
			log.debug("Executing: " + sql);
			conn.createStatement().execute(sql);
		}
		/*
		The index below may be used for more advanced JSON-specific indexing
		and querying, but currently we don't need this functionality

		String sql = new StringBuilder("CREATE INDEX ON ").append(table).append(" USING GIN(data jsonb_path_ops)").toString();
		log.debug("Executing: " + sql);
		conn.createStatement().execute(sql);
		*/

		log.debug("Creating indexes took " + (new Date().getTime() - before.getTime()) + " ms");
	}

	private boolean truncateTable() {
		Date before = new Date();
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = rowstore.getConnection();
			conn.setAutoCommit(false);
			stmt = conn.createStatement();

			log.debug("Truncating contents of table " + dataTable);
			String truncTable = "TRUNCATE " + dataTable;
			stmt.executeUpdate(truncTable);
			log.debug("Executing: " + truncTable);

			log.debug("Removing all indexes from table " + dataTable);
			Set<String> existingIndices = getIndexNames();
			for (String index : existingIndices) {
				// We cannot use prepared statements for CREATE INDEX with parametrized fields:
				// the type to be used with setObject() is not known and setString() does not work.
				// It should be safe to run BaseConnection.escapeString() to avoid SQL-injection
				String sql = new StringBuilder("DROP INDEX IF EXISTS ").append(index).toString();
				log.debug("Executing: " + sql);
				stmt.executeUpdate(sql);
			}

			conn.commit();
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
			try {
				log.info("Rolling back transaction");
				conn.rollback();
			} catch (SQLException e1) {
				SqlExceptionLogUtil.error(log, e1);
			}
			return false;
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}

			log.debug("Truncating table took " + (new Date().getTime() - before.getTime()) + " ms");
		}

		return true;
	}

	private Set<String> getIndexNames() {
		Date before = new Date();
		Set<String> result = new HashSet<>();
		Connection conn = null;
		ResultSet rs = null;
		Statement stmnt = null;
		try {
			conn = rowstore.getConnection();
			StringBuffer sql = new StringBuffer("SELECT ci.relname AS indexname ").
					append("FROM pg_index i,pg_class ci,pg_class ct ").
					append("WHERE i.indexrelid=ci.oid AND ").
					append("i.indrelid=ct.oid AND ").
					append("ct.relname='").append(dataTable).append("' AND ").
					append("ci.relname LIKE '%_jsonidx_%';"); // we only want our own indexes (no primary keys etc), so we filter for _jsonidx_ in the index name
			String sqlStr = sql.toString();
			stmnt = conn.createStatement();
			log.debug("Executing: " + sqlStr);
			rs = stmnt.executeQuery(sqlStr);
			while (rs.next()) {
				result.add(rs.getString("indexname"));
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (stmnt != null) {
				try {
					stmnt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}

			log.debug("Fetching index names took " + (new Date().getTime() - before.getTime()) + " ms");
		}

		return result;
	}

	/**
	 * @see Dataset#query(Map, int, int)
	 */
	@Override
	public QueryResult query(Map<String, String> tuples, int limit, int offset) {
		Date before = new Date();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String[] values = tuples.values().toArray(new String[tuples.size()]);

		List<JSONObject> result = new ArrayList<>();
		int resultCount = 0;
		int regexp = rowstore.getConfig().getRegexpQuerySupport();
		boolean optimizeRegexp = true;
		try {
			conn = rowstore.getQueryConnection();
			StringBuilder queryTemplate = new StringBuilder("SELECT data, count(*) OVER() AS result_count FROM " + getDataTable());
			if (tuples != null && tuples.size() > 0) {
				for (int i = 0; i < tuples.size(); i++) {
					// We check whether there is a value
					if (values[i].equals("~")) {
						log.debug("No value provided after ~");
						return new QueryResult.Error(Status.CLIENT_ERROR_BAD_REQUEST.getUri());
					}

					if (i == 0) {
						queryTemplate.append(" WHERE ");
					} else {
						queryTemplate.append(" AND ");
					}

					if (regexp == Dataset.REGEXP_QUERY_FULL && values[i].startsWith("~")) {
						optimizeRegexp = false;
					}

					// we match using ~ to enable regular expressions
					if (regexp == Dataset.REGEXP_QUERY_FULL && (!optimizeRegexp || DatasetUtil.isRegExpString(values[i]))) {
						queryTemplate.append("data->>? ~ ?");
					} else if (regexp == Dataset.REGEXP_QUERY_SIMPLE && values[i].startsWith("^")) {
						queryTemplate.append("data->>? ~ ?");
					} else {
						queryTemplate.append("data->>? = ?");
					}
				}
			}

			queryTemplate.append(" ORDER BY rownr LIMIT ? OFFSET ? ");

			stmt = conn.prepareStatement(queryTemplate.toString());

			int paramPos = 1;
			if (tuples != null && tuples.size() > 0) {
				Iterator<String> keys = tuples.keySet().iterator();
				while (keys.hasNext()) {
					String key = keys.next();
					stmt.setString(paramPos, key.toLowerCase());
					String value = tuples.get(key);
					if (!optimizeRegexp && value.startsWith("~")) {
						value = value.substring(1);
					}
					stmt.setString(paramPos + 1, value);
					paramPos += 2;
				}
			}

			stmt.setInt(paramPos++, limit);
			stmt.setInt(paramPos, offset);

			log.debug("Executing: " + stmt);

			int queryTO = rowstore.getConfig().getQueryTimeout();
			if (queryTO > -1) {
				stmt.setQueryTimeout(queryTO);
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				String value = rs.getString("data");
				if (resultCount == 0) {
					resultCount = rs.getInt("result_count");
				}
				try {
					result.add(new JSONObject(value));
				} catch (JSONException e) {
					log.error(e.getMessage());
				}
			}
		} catch (SQLException e) {
			//SqlExceptionLogUtil.error(log, e);
			log.debug(e.getMessage());
			return new QueryResult.Error(e.getSQLState());
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}

			log.debug("Performing query took " + (new Date().getTime() - before.getTime()) + " ms");
		}

		return new QueryResult(result, limit, offset, resultCount);
	}

	/**
	 * @see Dataset#getColumnNames()
	 */
	public Set<String> getColumnNames() {
		return this.getColumnNames(true);
	}

	private Set<String> getColumnNames(boolean useQueryDatabase) {
		Date before = new Date();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Set<String> result = new HashSet<>();
		try {
			if (useQueryDatabase) {
				conn = rowstore.getQueryConnection();
			} else {
				conn = rowstore.getConnection();
			}
			// FIXME the following query is very slow on large tables
			// (note: temporarily added WHERE clause to speed it up and avoid a full table scan,
			// side effect of WHERE clause is that eventually added data with different structure is not
			// being taken into consideration)
			//StringBuilder queryTemplate = new StringBuilder("SELECT DISTINCT jsonb_object_keys(data) AS column_names FROM " + getDataTable() + " WHERE rownr = '1'");
			StringBuilder queryTemplate = new StringBuilder("SELECT DISTINCT jsonb_object_keys(data) AS column_names FROM " + getDataTable() +
					" WHERE rownr=(SELECT min(rownr) FROM " + getDataTable() + ")");
			stmt = conn.prepareStatement(queryTemplate.toString());
			log.debug("Executing: " + stmt);

			rs = stmt.executeQuery();
			while (rs.next()) {
				result.add(rs.getString("column_names"));
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}

			log.debug("Fetching column names took " + (new Date().getTime() - before.getTime()) + " ms");
		}

		return result;
	}

	/**
	 * Initializes the object by loading all information from the database.
	 */
	private void initFromDb() {
		Date before = new Date();
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = rowstore.getConnection();
			stmt = conn.prepareStatement("SELECT * FROM " + PgDatasets.DATASETS_TABLE_NAME + " WHERE id = ?");
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(getId());
			stmt.setObject(1, uuid);
			log.info("Loading dataset " + getId() + " from database");
			log.debug("Executing: " + stmt);
			rs = stmt.executeQuery();
			if (rs.next()) {
				this.status = rs.getInt("status");
				this.created = rs.getTimestamp("created");
				this.dataTable = rs.getString("data_table");
			} else {
				throw new IllegalStateException("Unable to initialize Dataset object from database");
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
			throw new IllegalArgumentException(e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}

			log.debug("Loading dataset took " + (new Date().getTime() - before.getTime()) + " ms");
		}
	}

	/**
	 * @see Dataset#getRowCount()
	 */
	@Override
	public long getRowCount() {
		Date before = new Date();
		long result = -1;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = rowstore.getQueryConnection();
			stmt = conn.prepareStatement("SELECT COUNT(rownr)::BIGINT AS rowcount FROM " + getDataTable());
			log.debug("Executing: " + stmt);
			rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getLong("rowcount");
			}
			rs.close();
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}

			log.debug("Fetching row count took " + (new Date().getTime() - before.getTime()) + " ms");
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
		Date before = new Date();
		Set<String> result = new HashSet<>();;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			if (useQueryDatabase) {
				conn = rowstore.getQueryConnection();
			} else {
				conn = rowstore.getConnection();
			}
			stmt = conn.prepareStatement("SELECT * FROM " + PgDatasets.ALIAS_TABLE_NAME + " WHERE dataset_id = ?");
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(getId());
			stmt.setObject(1, uuid);
			log.info("Loading aliases for dataset " + getId());
			log.debug("Executing: " + stmt);
			rs = stmt.executeQuery();
			while (rs.next()) {
				String alias = rs.getString("alias");
				result.add(alias);
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}

			log.debug("Fetching aliases took " + (new Date().getTime() - before.getTime()) + " ms");
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
		Date before = new Date();
		Set<String> existingAliases = getAliases(false);
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = rowstore.getConnection();
			conn.setAutoCommit(false);

			ps = conn.prepareStatement("DELETE FROM " + PgDatasets.ALIAS_TABLE_NAME + " WHERE dataset_id = ?");
			PGobject uuid = new PGobject();
			uuid.setType("uuid");
			uuid.setValue(id);
			ps.setObject(1, uuid);
			log.debug("Executing: " + ps);
			ps.execute();
			ps.close();

			ps = conn.prepareStatement("INSERT INTO " + PgDatasets.ALIAS_TABLE_NAME + " (dataset_id, alias) VALUES (?, ?)");
			for (String alias : aliases) {
				if (existingAliases.contains(alias) || (isAliasValid(alias) && isAliasAvailable(alias))) {
					ps.setObject(1, uuid);
					ps.setString(2, alias);
					log.debug("Adding to batch: " + ps);
					ps.addBatch();
				} else {
					log.debug("Received invalid or unavailable alias, rolling back");
					conn.rollback();
					return false;
				}
			}
			log.debug("Executing and committing batch");
			ps.executeBatch();
			ps.close();
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				SqlExceptionLogUtil.error(log, e1);
			}
			log.error(e.getMessage());
			return false;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}

			log.debug("Setting aliases took " + (new Date().getTime() - before.getTime()) + " ms");
		}

		return true;
	}

	public String resolveAlias(String alias) {
		Date before = new Date();
		String result = null;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = rowstore.getConnection();
			stmt = conn.prepareStatement("SELECT * FROM " + PgDatasets.ALIAS_TABLE_NAME + " WHERE alias = ?");
			stmt.setString(1, alias);
			log.debug("Executing: " + stmt);
			rs = stmt.executeQuery();
			if (rs.next()) {
				UUID uuid = (UUID) rs.getObject("dataset_id");
				return uuid.toString();
			}
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					SqlExceptionLogUtil.error(log, e);
				}
			}

			log.debug("Resolving alias took " + (new Date().getTime() - before.getTime()) + " ms");
		}

		return result;
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
	private JSONObject csvLineToJsonObject(String[] line, String[] labels) throws JSONException {
		if (line.length > labels.length) {
			throw new IllegalArgumentException("Amount of values per row must not be higher than amount of labels in first row of CSV file");
		}

		JSONObject result = new JSONObject();
		for (int i = 0; i < line.length; i++) {
			// we skip empty strings as this would result in empty key names in the JSON result
			if (labels[i].trim().length() == 0) {
				continue;
			}
			result.put(labels[i], line[i]);
			putAndRetainLargestValue(labels[i], line[i].length());
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
			br = new BufferedReader(new AutoDetectReader(new FileInputStream(csvFile)));
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
		} catch (TikaException te) {
			log.info(te.getMessage());
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