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
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.RowStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

	protected PgDataset(RowStore rowstore, String id) {
		if (rowstore == null) {
			throw new IllegalArgumentException("RowStore must not be null");
		}
		if (id == null) {
			throw new IllegalArgumentException("Dataset ID must not be null");
		}
		this.rowstore = rowstore;
		this.id = id;
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
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = rowstore.getConnection();
			conn.setAutoCommit(true);
			stmt = conn.prepareStatement("UPDATE " + PgDatasets.TABLE_NAME + " SET status = ? WHERE id = ?");
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
	 * @see Dataset#populate(File)
	 */
	@Override
	public boolean populate(File csvFile) throws IOException {
		if (csvFile == null) {
			throw new IllegalArgumentException("Argument must not be null");
		}

		String dataTable = getDataTable();
		if (dataTable == null) {
			log.error("Dataset has no data table assigned");
			return false;
		}

		this.setStatus(EtlStatus.PROCESSING);

		Connection conn = null;
		PreparedStatement stmt = null;
		CSVReader cr = null;
		try {
			conn = rowstore.getConnection();
			cr = new CSVReader(new FileReader(csvFile), ',' , '"');
			int lineCount = 0;
			String[] labels = null;
			String[] line;

			conn.setAutoCommit(false);
			stmt = conn.prepareStatement("INSERT INTO " + dataTable + " (rownr, data) VALUES (?, ?)");
			while ((line = cr.readNext()) != null) {
				if (lineCount == 0) {
					labels = line;
				} else {
					try {
						JSONObject jsonLine = csvLineToJsonObject(line, labels);
						stmt.setInt(1, lineCount);
						PGobject jsonb = new PGobject();
						jsonb.setType("jsonb");
						jsonb.setValue(jsonLine.toString());
						stmt.setObject(2, jsonb);
						log.debug("Adding to batch: " + stmt);
						stmt.addBatch();
						// we execute the batch every 100th line
						if ((lineCount % 100) == 0) {
							stmt.executeBatch();
						}
					} catch (SQLException e) {
						SqlExceptionLogUtil.error(log, e);
						conn.rollback();
						return false;
					} catch (JSONException e) {
						log.error(e.getMessage());
						conn.rollback();
						return false;
					}
				}
				lineCount++;
			}
			// in case there are some inserts left to be sent (i.e.
			// batch size above was smaller than 100 when loop ended)
			log.debug("Executing: " + stmt);
			stmt.executeBatch();

			// we commit the transaction and free the resources of the statement
			conn.commit();

			// we create an index over the data
			createIndex(conn, dataTable, labels);

			setStatus(EtlStatus.AVAILABLE);
			return true;
		} catch (SQLException e) {
			SqlExceptionLogUtil.error(log, e);
			try {
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
		}
	}

	private void createIndex(Connection conn, String table, String[] fields) throws SQLException {
		/*
		The following works, but is prone to SQL-injection
		Sadly, prepared statements cannot be used for "CREATE INDEX"

		for (int i = 0; i < fields.length; i++) {
			String sql = new StringBuilder("CREATE INDEX ON ").append(table).
					append(" (").append("(data->>'").append(fields[i]).append("')").append(")").toString();
			log.debug("Executing: " + sql);
			Statement stmnt = conn.createStatement();
			stmnt.execute(sql);
		}
		*/
		String sql = new StringBuilder("CREATE INDEX ON ").append(table).append(" USING GIN(data jsonb_path_ops)").toString();
		log.debug("Executing: " + sql);
		conn.createStatement().execute(sql);
		conn.commit();
	}

	/**
	 * @see Dataset#query(Map)
	 */
	@Override
	public List<JSONObject> query(Map<String, String> tuples) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		boolean regexp = rowstore.getConfig().hasRegExpQuerySupport();
		List<JSONObject> result = new ArrayList<>();
		try {
			conn = rowstore.getConnection();
			StringBuilder queryTemplate = new StringBuilder("SELECT data FROM " + getDataTable());
			if (tuples != null && tuples.size() > 0) {
				for (int i = 0; i < tuples.size(); i++) {
					if (i == 0) {
						queryTemplate.append(" WHERE ");
					} else {
						queryTemplate.append(" AND ");
					}
					if (regexp) {
						// we match using ~ to enable regular expressions
						queryTemplate.append("data->>? ~ ?");
					} else {
						queryTemplate.append("data->>? = ?");
					}
				}
			}

			stmt = conn.prepareStatement(queryTemplate.toString());

			if (tuples != null && tuples.size() > 0) {
				Iterator<String> keys = tuples.keySet().iterator();
				int paramPos = 1;
				while (keys.hasNext()) {
					String key = keys.next();
					stmt.setString(paramPos, key);
					stmt.setString(paramPos + 1, tuples.get(key));
					paramPos += 2;
				}
			}

			log.debug("Executing: " + stmt);

			rs = stmt.executeQuery();
			while (rs.next()) {
				String value = rs.getString("data");
				try {
					result.add(new JSONObject(value));
				} catch (JSONException e) {
					log.error(e.getMessage());
				}
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
		}

		return result;
	}

	/**
	 * @see Dataset#getColumnNames()
	 */
	@Override
	public Set<String> getColumnNames() {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Set<String> result = new HashSet<>();
		try {
			conn = rowstore.getConnection();
			StringBuilder queryTemplate = new StringBuilder("SELECT * FROM " + getDataTable() + " LIMIT 1");
			stmt = conn.prepareStatement(queryTemplate.toString());
			log.debug("Executing: " + stmt);

			rs = stmt.executeQuery();
			if (rs.next()) {
				String strRow = rs.getString("data");
				try {
					JSONObject jsonRow = new JSONObject(strRow);
					Iterator<String> keys = jsonRow.keys();
					while (keys.hasNext()) {
						result.add(keys.next());
					}
				} catch (JSONException e) {
					log.error(e.getMessage());
				}
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
		}

		return result;
	}

	/**
	 * Initializes the object by loading all information from the database.
	 */
	private void initFromDb() {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = rowstore.getConnection();
			stmt = conn.prepareStatement("SELECT * FROM " + PgDatasets.TABLE_NAME + " WHERE id = ?");
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
				throw new IllegalStateException("Unable to initialize Database object from database");
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
		}
	}

	/**
	 * @see Dataset#getRowCount()
	 */
	@Override
	public int getRowCount() {
		int result = -1;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = rowstore.getConnection();
			stmt = conn.prepareStatement("SELECT COUNT(rownr) AS rowcount FROM " + getDataTable());
			log.debug("Executing: " + stmt);
			rs = stmt.executeQuery();
			while (rs.next()) {
				result = rs.getInt("rowcount");
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
		}

		return result;
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
		if (line.length != labels.length) {
			throw new IllegalArgumentException("Arrays must not be of different length");
		}

		JSONObject result = new JSONObject();
		for (int i = 0; i < line.length; i++) {
			result.put(labels[i], line[i]);
		}

		return result;
	}

}