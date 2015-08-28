package org.entrystore.rowstore.store.impl;

import com.opencsv.CSVReader;
import org.entrystore.rowstore.etl.EtlStatus;
import org.entrystore.rowstore.store.Dataset;
import org.entrystore.rowstore.store.RowStore;
import org.json.JSONException;
import org.json.JSONObject;
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
 * @author Hannes Ebner
 */
public class PgDataset implements Dataset {

	private static Logger log = LoggerFactory.getLogger(PgDataset.class);

	private String id;

	private int status;

	private Date created;

	private String dataTable;

	private RowStore rowstore;

	protected PgDataset(RowStore rowstore, String id) {
		this.rowstore = rowstore;
		this.id = id;
		initFromDb();
	}

	protected PgDataset(RowStore rowstore, String id, int status, Date created) {
		this.rowstore = rowstore;
		this.id = id;
		this.status = status;
		this.created = created;
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public int getStatus() {
		// we reload the info from the DB because the status may have changed
		initFromDb();
		return status;
	}

	@Override
	public void setStatus(int status) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = rowstore.getConnection();
			conn.setAutoCommit(true);
			stmt = conn.prepareStatement("UPDATE ? SET status = ? WHERE id = ?");
			stmt.setString(1, PgDatasets.TABLE_NAME);
			stmt.setInt(2, status);
			stmt.setString(3, getId());
			stmt.executeUpdate();
		} catch (SQLException e) {
			log.error(e.getMessage());
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
		}

		this.status = status;
	}

	@Override
	public Date getCreationDate() {
		return created;
	}

	private String getDataTable() {
		return dataTable;
	}

	@Override
	public boolean populate(File csvFile) throws IOException {
		if (csvFile == null) {
			throw new IllegalArgumentException("Argument must not be null");
		}

		String dataTable = getDataTable();
		if (dataTable == null) {
			log.error("");
			return false;
		}

		this.setStatus(EtlStatus.PROCESSING);

		Connection conn = null;
		PreparedStatement stmt = null;
		CSVReader cr = null;
		try {
			conn = rowstore.getConnection();
			cr = new CSVReader(new FileReader(csvFile));
			long lineCount = 0;
			String[] labels = null;
			String[] line;

			conn.setAutoCommit(false);
			stmt = conn.prepareStatement("INSERT INTO ? (row, data) VALUES (?, ?)");
			while ((line = cr.readNext()) != null) {
				if (lineCount == 0) {
					labels = line;
				} else {
					try {
						JSONObject jsonLine = csvLineToJsonObject(line, labels);
						stmt.setString(1, dataTable);
						stmt.setString(2, jsonLine.toString());
						stmt.addBatch();
						// we execute the batch every 100th line
						if ((lineCount % 100) == 0) {
							stmt.executeBatch();
						}
					} catch (SQLException e) {
						log.error(e.getMessage());
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
			stmt.executeBatch();

			// we commit the transaction and free the resources of the statement
			conn.commit();
			this.setStatus(EtlStatus.AVAILABLE);
			return true;
		} catch (SQLException e) {
			log.error(e.getMessage());
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.error(e1.getMessage());
			}
			this.setStatus(EtlStatus.ERROR);
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
					log.error(e.getMessage());
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	@Override
	public List<JSONObject> query(Map<String, String> tuples) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		boolean regexp = rowstore.getConfig().hasRegExpQuerySupport();
		List<JSONObject> result = new ArrayList<>();
		try {
			conn = rowstore.getConnection();
			StringBuilder queryTemplate = new StringBuilder("SELECT * FROM ? WHERE id = ?");
			if (tuples != null && tuples.size() > 0) {
				for (int i = 0; i < tuples.size(); i++) {
					if (regexp) {
						// we match using ~ to enable regular expressions
						queryTemplate.append(" AND data->>? ~ ?");
					} else {
						queryTemplate.append(" AND data->>? = ?");
					}
				}
			}

			stmt = conn.prepareStatement(queryTemplate.toString());
			stmt.setString(1, getDataTable());
			stmt.setString(2, getId());

			if (tuples != null && tuples.size() > 0) {
				Iterator<String> keys = tuples.keySet().iterator();
				int paramPos = 3;
				while (keys.hasNext()) {
					String key = keys.next();
					stmt.setString(paramPos, key);
					stmt.setString(paramPos + 1, tuples.get(key));
					paramPos += 2;
				}
			}

			log.info("Executing query: " + stmt);

			rs = stmt.executeQuery();
			int columnCount = rs.getMetaData().getColumnCount();
			if (rs.next()) {
				for (int i = 1; i <= columnCount; i++) {
					String key = rs.getMetaData().getColumnName(i);
					String value = rs.getString(i);
					JSONObject row = new JSONObject();
					try {
						row.put(key, value);
					} catch (JSONException e) {
						log.error(e.getMessage());
					}
					result.add(row);
				}
			}
		} catch (SQLException e) {
			log.error(e.getMessage());
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
		}

		return result;
	}

	@Override
	public Set<String> getColumnNames() {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Set<String> result = new HashSet<>();
		try {
			conn = rowstore.getConnection();
			StringBuilder queryTemplate = new StringBuilder("SELECT * FROM ? WHERE id = ? LIMIT 1");
			stmt = conn.prepareStatement(queryTemplate.toString());
			stmt.setString(1, getDataTable());
			stmt.setString(2, getId());
			log.info("Executing query: " + stmt);

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
			log.error(e.getMessage());
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
		}

		return result;
	}

	private void initFromDb() {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = rowstore.getConnection();
			stmt = conn.prepareStatement("SELECT * FROM ? WHERE id = ?");
			stmt.setString(1, PgDatasets.TABLE_NAME);
			stmt.setString(2, getId());
			rs = stmt.executeQuery();
			if (rs.next()) {
				this.status = rs.getInt("status");
				this.created = rs.getTimestamp("created");
				this.dataTable = rs.getString("data_table");
			}
			rs.close();
		} catch (SQLException e) {
			log.error(e.getMessage());
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	public JSONObject csvLineToJsonObject(String[] line, String[] labels) throws JSONException {
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