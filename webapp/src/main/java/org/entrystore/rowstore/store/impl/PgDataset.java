package org.entrystore.rowstore.store.impl;

import com.opencsv.CSVReader;
import org.entrystore.rowstore.etl.ConverterUtil;
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
import java.util.Date;

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
						JSONObject jsonLine = ConverterUtil.csvLineToJsonObject(line, labels);
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

	private void initFromDb() {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = rowstore.getConnection();
			stmt = conn.prepareStatement("SELECT * FROM ? WHERE id = ?");
			stmt.setString(1, PgDatasets.TABLE_NAME);
			stmt.setString(2, getId());
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				this.status = rs.getInt("status");
				this.created = rs.getTimestamp("created");
				this.dataTable = rs.getString("data_table");
			}
			rs.close();
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
	}

}